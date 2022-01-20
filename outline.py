# Imports
from collections import defaultdict
from flask import Flask, request, Response
import json, os, requests, sys, time

# Create app
app = Flask(__name__)

# Storage data structures
# ---------------------------------------------------------------------------
key_dict = {}
replicas = []
socket = os.environ.get('SOCKET_ADDRESS') # Replica gets its own IP and port
vclock = defaultdict(int)
if not socket:
    sys.exit('No socket address specified????')
if os.environ.get('VIEW'):
    replicas = os.environ.get('VIEW').split(',') # Replica gets other IPs and Ports

# Replica just created, broadcasts PUT request to all other replicas in view
# If timeout: assume timeout'd replica has gone down, broadcast DELETE 
# Random broadcasting
def broadcast_delete(down_replicas):
    global replicas
    while down_replicas:
        sock_to_del_json = {'socket-address': down_replicas.pop(0)}
        for replica in replicas:
            if replica == socket:
                continue
            try:
                requests.delete(f'http://{replica}/key-value-store-view', json = json.dumps(sock_to_del_json))
            except:
                print(f'Error, timeout deleting socket from {replica}')
                down_replicas.append(replica)
    # DEBUG
    print("printing replicas that are up")
    for replica in replicas:
        print(replica, " ")

    if down_replicas:
        print("UH OH! down_replicas in broadcast_delete:")
        for down_replica in down_replicas:
            print(down_replica)
            if down_replica in replicas:
                replicas.remove(down_replica)
        broadcast_delete(down_replicas)
    

def start_up_broadcast():
    global key_dict, replicas
    down_replicas = []
    for replica in replicas:
        print(f"Loop in start_up_broadcast for {replica}")
        if replica == socket:
            continue
        sock_to_add_json = {'socket-address': socket}

        try:
            # Add this replica to other replicas' views
            requests.put(f'http://{replica}/key-value-store-view', json = json.dumps(sock_to_add_json))
            
            # Retrieve key-value dictionary from a running replica
            if not key_dict:
                broadcast_get = requests.get(f'http://{replica}/key-value-store').json()
                print(f'GET Response from {replica} during key value retrieval startup:{broadcast_get}')
                key_dict = dict(broadcast_get['store'])
                vclock = dict(broadcast_get['causal-metadata'])
        except Exception as e:
            # print(f'Error, timeout adding socket to {replica} during startup broadcasts\n')
            # Broadcast delete here.
            down_replicas.append(replica)
            continue

    for down_replica in down_replicas:
        if down_replica in replicas:
            replicas.remove(down_replica)
            print(f"{down_replica} removed from replicas")

    broadcast_delete(down_replicas)

# Start up broadcast
start_up_broadcast()

print("DOES THIS REPEAT: looking for bug that causes requests to duplicate")

for replica in replicas:
    vclock[replica] = 0
print(vclock)

# NOTE: current thought: replicas are updated everytime a client sends a req, as a broadcast 
# is sent to all replicas to note of the change

# Key Value Store Endpoint
# Requests from Client to a Replica
# ----------------------------------------------------------------------------
@app.route('/key-value-store', methods=['GET'])
def main_path():
    #req_json = request.get_json()
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    if request.method == 'GET':
        resp_json = {
            'store': key_dict,
            'causal-metadata': vclock
        }
        status = 200
    return Response(json.dumps(resp_json), status, mimetype="application/json")

# This may need to be reworked. I think keeping a record of previously received
# Causal meta data might be good.
def compare_vclocks(sender_clock, receiver_clock):
    # Check that for the keys the client has
        # The receiver has the client's keys
        # The receiver is allowed to have more keys than the client
    replicas_from_sender = list(sender_clock.keys())
    replicas_in_receiver = list(receiver_clock.keys())
    if not all(x in replicas_in_receiver for x in replicas_from_sender):
        return False
    # Check that for the matching key
    for replica in sender_clock:
        #if replica == socket:
        #    continue
        if not sender_clock[replica] <= receiver_clock[replica]:
            return False
    return True

#Given the sender's clock is ahead of the recvr's clock, return the list of replicas
#that may have the up to date key-value data.
#Precondition: compare_vclocks(sender, receiver) == False
def get_ahead_vclocks(sender_clock, receiver_clock):
    ret = []
    for replica in sender_clock:
        if replica not in receiver_clock.keys():
            ret.append(replica)
        elif sender_clock[replica] > receiver_clock[replica]:
            ret.append(replica)
    return ret

def merge_vclock(sender_clock):
    global vclock
    for replica in sender_clock:
        if replica in vclock:
            vclock[replica] = max(vclock[replica], sender_clock[replica])
        else:
            vclock[replica] = sender_clock[replica]

# Still need to generate causal meta data
@app.route('/key-value-store/<key>', methods=['GET', 'PUT', 'DELETE'])
def key_func(key):
    global key_dict, replicas, vclock

    try:
        req_json = request.get_json()
    except:
        req_json = None

    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    count = 0
    if req_json and req_json.get('causal-metadata'):
        caus_meta = req_json.get('causal-metadata')
        if (caus_meta == ""):
            caus_meta = dict()
        # while not compare_vclocks(caus_meta, vclock): # Sleep/Busy wait until vclock is up to date, MAY need to add a timeout
        #     time.sleep(0.01)
        #     count += 1
        #     if count == (1000):
        #         return f'Busy wait fail vclock: {vclock}\n sender vclock: {caus_meta}'
        if not compare_vclocks(caus_meta, vclock):
            to_query = get_ahead_vclocks(caus_meta, vclock)
            # app.logger.info(to_query)
            #to_query are the replicas that have a vclock entry that indicates they have stuff we don't
            #this replica will now attempt to update
            for replica in to_query:
                try: 
                    resp = requests.get(f'http://{replica}/key-value-store-consistency', timeout=20)
                    key_dict = resp.json()['store']
                    # vclock = resp.json()['causal-metadata'] #TODO maybe merge incoming caus_meta with existing vclock?
                    merge_vclock(resp.json()['causal-metadata'])
                except Exception as e: # need to broadcast delete
                    app.logger.info(f'{replica} is down?')
                    app.logger.info(e)
                if compare_vclocks(caus_meta, vclock):
                    #we are now up to date enough to handle the incoming request
                    break

    # app.logger.info(key_dict)

    # TODO: before anything, check casual metadata in req
    # TODO: Process QUEUE

    # NOTE: for GET, we do not need to communicate with other replicas
    if request.method == 'GET':
        if key in key_dict:
            resp_json = {
                #'doesExist': True, 
                'message': 'Retrieved successfully', 
                'value': key_dict[key],
                'causal-metadata': vclock
            }
            status = 200
        else:
            resp_json = {
                #'doesExist': False, 
                'error': 'Key does not exist', 
                'message': 'Error in GET'
                }
            status = 404
        return Response(json.dumps(resp_json), status, mimetype='application/json')
    
    # TODO: need to update vclock, update key_dict in all other replicas with timeout
    elif request.method == 'PUT':
        down_replicas = []
        if req_json:
            if 'value' not in req_json or key is None: # This line MAY need to be changed
                resp_json = {
                    'error':'Value is missing',
                    'message':'Error in PUT'
                }
                status = 400
            elif len(key) > 50:
                resp_json = {
                    'error':'Key is too long',
                    'message':'Error in PUT'
                }
                status = 400
            elif key not in key_dict:
                key_dict[key] = req_json['value']
                vclock[socket] += 1
                resp_json = {
                    'message':'Added successfully',
                    #'replaced': False
                    'causal-metadata': vclock
                }
                status = 201
                for replica in replicas:
                    # update key_dict in all other replicas
                    if replica == socket:
                        continue
                    try: 
                        requests.put(f'http://{replica}/key-value-store-consistency/{key}', 
                        json=json.dumps({
                            'value': req_json['value'], 
                            'causal-metadata': vclock,
                            'sender': socket
                            }), timeout=20)
                        
                    except: # need to broadcast delete
                        down_replicas.append(replica)
                for down_replica in down_replicas:
                    replicas.remove(down_replica)
                for down_replica in down_replicas: # A function and loop should be made to continually check that a replica is not down and handle exceptions
                    #replicas.remove(down_replica)  # This is a bandaid fix right now 
                    for replica in replicas:
                        requests.delete(f'http://{replica}/key-value-store-view',
                            json=json.dumps({
                                'socket-address': down_replica
                            }), timeout=20)
                        if down_replica in replicas:
                            replicas.remove(down_replica)
                
            else:
                key_dict[key] = req_json['value']
                vclock[socket] += 1
                # update key_dict in all other replicas and update vclock
                resp_json = {
                    'message':'Updated successfully', 
                    #'replaced': True
                    'causal-metadata': vclock
                }
                status = 200
                for replica in replicas:
                    # update key_dict in all other replicas and update vclock
                    if replica == socket:
                        continue
                    try: 
                        requests.put(f'http://{replica}/key-value-store-consistency/{key}', 
                        json=json.dumps({
                            'value': req_json['value'], 
                            'causal-metadata': vclock,
                            'sender': socket
                            }), timeout=20)
                        
                    except: # need to broadcast delete
                        down_replicas.append(replica)
                for down_replica in down_replicas:
                    replicas.remove(down_replica)
                for down_replica in down_replicas:
                    for replica in replicas:
                        requests.delete(f'http://{replica}/key-value-store-view',
                            json=json.dumps({
                                'socket-address': down_replica
                            }), timeout=20)
                        if down_replica in replicas:
                            replicas.remove(down_replica)
    elif request.method == 'DELETE':
        if key in key_dict:
            del key_dict[key]
            vclock[socket] += 1
            resp_json = {
                #'doesExist' : True,
                'message'   : 'Deleted successfully',
                'causal-metadata': vclock
            }
            status = 200
            for replica in replicas:
                # update key_dict in all other replicas and update vclock
                if replica == socket:
                    continue
                try: 
                    requests.delete(f'http://{replica}/key-value-store-consistency/{key}', 
                    json=json.dumps({'causal-metadata': vclock, 'sender': socket}), timeout=20)
                except: # need to broadcast delete
                    down_replicas.append(replica)
            for down_replica in down_replicas:
                replicas.remove(down_replica)
            for down_replica in down_replicas:
                for replica in replicas:
                    requests.delete(f'http://{replica}/key-value-store-view', json=json.dumps({'socket-address': down_replica}), timeout=20)
                    if down_replica in replicas:
                        replicas.remove(down_replica)
        else:
            resp_json = {
                'doesExist' : False,
                'error'     : 'Key does not exist',
                'message'   : 'Error in DELETE'
            }
            status = 404
        pass

    return Response(json.dumps(resp_json), status, mimetype='application/json')
    

# Need to determine how we want a new replica to send out a PUT request to all
# replicas when it is created

"""
Something like this is suitable:
for socket in replicas:
    requests.put(f'http://{forward_add}/key-value-store/{key}', json = request.get_json())
Would likely put this after Storage Data Structures,also need to populate the key/val store
"""

# View Endpoint
# ----------------------------------------------------------------------------
@app.route('/key-value-store-view', methods=['GET', 'PUT', 'DELETE'])
def replica_view():
    global replicas
    try:
        req_json = request.get_json()
    except:
        req_json = {}
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    
    if request.method == 'GET':
        resp_json = {
            'message': 'View retrieved successfully', 
            'view': ','.join(replicas)
        }
        status = 200
    elif request.method == 'PUT':
        if req_json:
            req_json = json.loads(req_json)
            if req_json.get('socket-address') in replicas:
                resp_json = {
                    'error': 'Socket address already exists in the view',
                    'message': 'Error in PUT'
                }
                status = 404
            elif req_json.get('socket-address'):
                replicas.append(req_json.get('socket-address'))
                if not req_json.get('socket-address') in vclock:
                    vclock[req_json.get('socket-address')] = 0
                replicas.sort()
                resp_json = {
                    'message': 'Replica added successfully to the view'
                }
                status = 201
    elif request.method == 'DELETE':
        if req_json:
            req_json = json.loads(req_json)
            try:
                replicas.remove(req_json.get('socket-address'))
                resp_json = {
                    'message': 'Replica deleted successfully from the view'
                }
                status = 200
            except ValueError:
                resp_json = {
                    'error': 'Socket address does not exist in the view',
                    'message': 'Error in DELETE'
                }
                status = 404
    
    return Response(json.dumps(resp_json), status, mimetype='application/json')
  
# Consistency Endpoint
# Requests from Replica to a Replica
# GET: get a replica's vclock and key_dict, 
# PUT: payload: vclock and key_dict, check own vclock on whether or not to 'deliver' i.e. update own key_dict and vector clock
# DELETE: payload: vclock and key_dict, check own vclock on whether or not to 'deliver' i.e. delete key in own key_dict and update vector clock
# Q: should we return a response? yes. need one to timeout
# ----------------------------------------------------------------------------
def internal_causal_check(sender_causal_metadata, sender_socket):
    #vclock_in = req_json['vclock']
    #addr_in = req_json['addr']
    #value = req_json['value']
    
    # Check if able to deliver based on vector clock values
    no_missing_msgs = True
    for replica in replicas:
        if replica == sender_socket:
            continue
        if not sender_causal_metadata[replica] <= vclock[replica]: # Key error
            no_missing_msgs = False
            break
    if no_missing_msgs and sender_causal_metadata[sender_socket] == vclock[sender_socket] + 1:
        # Set internal vclock to pointwise maximum
        for v in vclock: # where v is each key
            vclock[v] = max(vclock[v], sender_causal_metadata[v])
        return True
        
    return False

@app.route('/key-value-store-consistency/<key>', methods=['GET', 'PUT', 'DELETE'])
def replica_to_replica_consistency(key):
    global key_dict, replicas, vclock
    try:
        req_json = json.loads(request.get_json())
    except:
        req_json = None

    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    count = 0
    if req_json and req_json.get('causal-metadata') and req_json.get('sender'):
        caus_meta = req_json.get('causal-metadata')
        if (caus_meta == ""):
            caus_meta = dict()
        sender = req_json.get('sender')
        while not internal_causal_check(caus_meta, sender): # Sleep/Busy wait until vclock is up to date, MAY need to add a timeout
            time.sleep(0.01)
            count += 1
            if count == (1000):
                return f'Busy wait fail vclock: {vclock}\n sender vclock: {caus_meta}'
    # Do not think we need the GET endpoint 
    """if request.method == 'GET':
        if key in key_dict:
            resp_json = {
                'doesExist': True, 
                'message': 'Retrieved successfully', 
                'value': key_dict[key]
            }
            status = 200
        else:
            resp_json = {
                'doesExist': False, 
                'error': 'Key does not exist', 
                'message': 'Error in GET'
                }
            status = 404
        return Response(json.dumps(resp_json), status, mimetype='application/json')
    """
    # NOTE: need to update vclock, update key_dict in all other replicas with timeout
    # TODO
    if request.method == 'PUT':
        #if req_json:
        req_json = dict(req_json)
        #vclock_in = req_json['vclock']
        #addr_in = req_json['addr']
        value = req_json['value']
        
        # Check if able to deliver based on vector clock values
        #no_missing_msgs = True
        #for replica in replicas:
        #    if replica == addr_in:
        #        continue
        #    if not vclock_in[replica] <= vclock[replica]:
        #        no_missing_msgs = False
        #        break
        #if no_missing_msgs and vclock_in[addr_in] == vclock[addr_in] + 1:
            # Able to deliver immediately
        #for v in vclock:
        #    vclock[v] = max(vclock[v], vclock_in[v])
        key_dict[key] = value
        resp_json = {
            'message': f'Successfully replicated PUT in replica {socket}'
        }
        status = 200
        #else : 
            # Need to add to queue
            # TODO
        #    resp_json = {
        #        'message': 'PUT queued in replica {socket}', 
        #    }
        #    status = 200

    elif request.method == 'DELETE':
        #if req_json:
        req_json = dict(req_json)
        #vclock_in = req_json['vclock']
        #addr_in = req_json['addr']

        # Check if able to deliver based on vector clock values
        """no_missing_msgs = True
        for v in vclock:
            if v == addr_in:
                continue
            if not vclock_in[v] <= vclock[v]:
                no_missing_msgs = False
                break
        if no_missing_msgs and vclock_in[addr_in] == vclock[addr_in] + 1:
            # Able to deliver immediately"""
        #for v in vclock:
        #    vclock[v] = max(vclock[v], vclock_in[v])
        if key in key_dict:
            del key_dict[key]
            resp_json = {
                'message': f'Successfully replicated DELETE in replica {socket}'
            }
            status = 200
        else:
            resp_json = {
                'message': f'Unable to replicate DELETE in replica {socket}',
                'error': 'No key found'
            }
            status = 404
        """else : 
            # Need to add to queue
            # TODO
            resp_json = {
            'message': 'DELETE queued in replica {socket}', 
            }
            status = 200"""
    return Response(json.dumps(resp_json), status, mimetype='application/json')

@app.route('/key-value-store-consistency', methods=['GET'])
def replica_key_value_copy():
    resp_json = {'message': 'This method is unsupported.'}
    status = 405
    if request.method == 'GET':
        resp_json = {
            'message': f'Key-value data from replica {socket}',
            'store': key_dict,
            'causal-metadata': vclock
        }
        status = 200
    return Response(json.dumps(resp_json), status, mimetype='application/json')


"""
if not forward_add:
    @app.route('/key-value-store/<key>', methods=['GET', 'PUT', 'DELETE'])
    def key_func(key):
        #app.logger.info(request.__dict__)
        #app.logger.info(request.get_json())
        resp_json = dict()
        status = 405
        if request.method == 'GET':
            if key in key_dict:
                resp_json = {
                    "doesExist": True, 
                    "message": "Retrieved successfully", 
                    "value": key_dict[key]
                }
                status = 200
            else:
                resp_json = {
                    "doesExist": False, 
                    "error": "Key does not exist", 
                    "message": "Error in GET"
                    }
                status = 404
            return Response(json.dumps(resp_json), status, mimetype="application/json")
        elif request.method == 'PUT':
            req_json = request.get_json()
            if 'value' not in req_json or key is None:
                resp_json = {
                    'error':'Value is missing',
                    'message':'Error in PUT'
                }
                status = 400
            elif len(key) > 50:
                resp_json = {
                    'error':'Key is too long',
                    'message':'Error in PUT'
                }
                status = 400
            elif key not in key_dict:
                key_dict[key] = req_json["value"]
                resp_json = {
                    'message':'Added successfully',
                    'replaced':False
                }
                status = 201
            else:
                key_dict[key] = req_json["value"]
                resp_json = {
                    'message':'Updated successfully', 
                    'replaced':True
                }
                status = 200
            return Response(json.dumps(resp_json), status, mimetype='application/json')
        elif request.method == 'DELETE':
            if key in key_dict:
                del key_dict[key]
                resp_json = {
                    'doesExist' : True,
                    'message'   : 'Deleted successfully'
                }
                status = 200
            else:
                resp_json = {
                    'doesExist' : False,
                    'error'     : 'Key does not exist',
                    'message'   : 'Error in DELETE'
                }
                status = 404
            return Response(json.dumps(resp_json), status, mimetype='application/json')
        return 'This method is unsupported.'
else:
    @app.route('/key-value-store/<key>', methods=['GET', 'PUT', 'DELETE'])
    def key_func(key):
        forward_resp = None
        error_resp = {
            'error': 'Main instance is down'
        }
        if request.method == 'GET':
            try:
                forward_resp = requests.get(f'http://{forward_add}/key-value-store/{key}')
            except:
                error_resp['message'] = 'Error in GET'
                return Response(json.dumps(error_resp), 503, mimetype='application/json')
            return Response(forward_resp.text, forward_resp.status_code, mimetype='application/json')
        elif request.method == 'PUT':
            #app.logger.info(req_json)
            try:
                forward_resp = requests.put(f'http://{forward_add}/key-value-store/{key}', json = request.get_json())
            except:
                error_resp['message'] = 'Error in PUT'
                return Response(json.dumps(error_resp), 503, mimetype='application/json')
            # print(forward_resp)
            return Response(forward_resp.text, forward_resp.status_code, mimetype='application/json')
        elif request.method == 'DELETE':
            try:
                forward_resp = requests.delete(f'http://{forward_add}/key-value-store/{key}')
            except:
                error_resp['message'] = 'Error in DELETE'
                return Response(json.dumps(error_resp), 503, mimetype='application/json')
            # print(forward_resp)
            return Response(forward_resp.text, forward_resp.status_code, mimetype='application/json')
        return 'This method is unsupported.'
"""

@app.route('/test1', methods=['GET', 'PUT', 'DELETE'])
def testing1():
    while(True):
        continue
    return

@app.route('/test2', methods=['GET', 'PUT', 'DELETE'])
def testing2():
    return 1

# Main Code to run
if __name__ == "__main__":
    app.run(host = '0.0.0.0', port = 8085, debug = True, threaded=True, use_reloader=False)