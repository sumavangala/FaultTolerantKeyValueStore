/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	ring.clear();
	ring = curMemList;
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(ht->currentSize() > 0){
		stabilizationProtocol();
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	g_transID++;
	Message *mesg;

	vector<Node> replicas = findNodes(key);

	for(vector<Node>::iterator it = replicas.begin(); it!=replicas.end(); it++){
		mesg = new Message(g_transID, memberNode->addr, MessageType::CREATE, key, value, ReplicaType(distance(replicas.begin(), it)));
		emulNet->ENsend(&(memberNode->addr), (*(it)).getAddress(), mesg->toString());
	}
	statusHT->create(to_string(g_transID),"CREATE::" + to_string(0) + "::" + to_string(0) + "::" + key + "::" + value);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	g_transID++;
	Message *mesg;
	updateRing();
	cout<< to_string(ring.size()) + "\t";
	vector<Node> replicas = findNodes(key);
	for(vector<Node>::iterator it = replicas.begin(); it!=replicas.end(); it++){
		mesg = new Message(g_transID, memberNode->addr, MessageType::READ, key);
		emulNet->ENsend(&(memberNode->addr), (*(it)).getAddress(), mesg->toString());
	}
	statusHT->create(to_string(g_transID),"READ::" + to_string(0) + "::" + to_string(0) + "::" + key + "::" + "");
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	/*g_transID++;
	Message *mesg;

	vector<Node> replicas = findNodes(key);

	for(vector<Node>::iterator it = replicas.begin(); it!=replicas.end(); it++){
		mesg = new Message(g_transID, memberNode->addr, MessageType::UPDATE, key, value, ReplicaType(distance(replicas.begin(), it)));
		emulNet->ENsend(&(memberNode->addr), (*(it)).getAddress(), mesg->toString());
	}
	statusHT->create(to_string(g_transID),to_string(0));*/
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	g_transID++;
	Message *mesg;

	vector<Node> replicas = findNodes(key);

	for(vector<Node>::iterator it = replicas.begin(); it!=replicas.end(); it++){
		mesg = new Message(g_transID, memberNode->addr, MessageType::DELETE, key);
		emulNet->ENsend(&(memberNode->addr), (*(it)).getAddress(), mesg->toString());
	}
	statusHT->create(to_string(g_transID),"DELETE::" + to_string(0) + "::" + to_string(0) + "::" + key + "::" + "");
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID, Address coodAddr) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table

	Entry *en = new Entry(value, par->getcurrtime(), replica);
	bool result = ht->create(key,en->convertToString());
	Message *mesg;

	if(result)
		log->logCreateSuccess(&(memberNode->addr), false, transID, key, value);
	else
		log->logCreateFail(&(memberNode->addr), false, transID, key, value);

	mesg = new Message(transID, memberNode->addr, MessageType::REPLY, result);
	emulNet->ENsend(&(memberNode->addr), &coodAddr, mesg->toString());

	findNeighbors();
	return result;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID, Address coodAddr) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	string result = ht->read(key);
	Message *mesg;

	if(!result.empty()){
		Entry *en = new Entry(result);
		log->logReadSuccess(&(memberNode->addr), false, transID, key, en->value);
	}
	else
		log->logReadFail(&(memberNode->addr), false, transID, key);

	mesg = new Message(transID, memberNode->addr, result);
	emulNet->ENsend(&(memberNode->addr), &coodAddr, mesg->toString());

	return result;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID, Address coodAddr) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	/*Entry *en = new Entry(value, par->getcurrtime(), replica);
	bool result = ht->update(key,en->convertToString());
	Message *mesg;

	if(result)
		log->logUpdateSuccess(&(memberNode->addr), false, transID, key, value);
	else
		log->logUpdateFail(&(memberNode->addr), false, transID, key, value);

	mesg = new Message(transID, memberNode->addr, MessageType::REPLY, result);
	emulNet->ENsend(&(memberNode->addr), &coodAddr, mesg->toString());

	return result;*/
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID, Address coodAddr) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	bool result = ht->deleteKey(key);
	Message *mesg;

	if(result)
		log->logDeleteSuccess(&(memberNode->addr), false, transID, key);
	else
		log->logDeleteFail(&(memberNode->addr), false, transID, key);

	mesg = new Message(transID, memberNode->addr, MessageType::REPLY, result);
	emulNet->ENsend(&(memberNode->addr), &coodAddr, mesg->toString());

	return result;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */
	bool result;
	string val;
	size_t pos;
	size_t start;
	int successCount = 0, failureCount = 0;
	Message *mesg;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		mesg = new Message(message);
		vector<string> tuple;

		switch(mesg->type)
		{
			case CREATE:
				createKeyValue(mesg->key, mesg-> value, mesg->replica, mesg->transID, mesg->fromAddr);
				break;
			case DELETE:
				deletekey(mesg->key, mesg->transID, mesg->fromAddr);
				break;
			case READ:
				readKey(mesg->key, mesg->transID, mesg->fromAddr);
				break;
			case UPDATE:
				//result = updateKeyValue(mesg->key, mesg-> value, mesg->replica, mesg->transID, mesg->fromAddr);
				break;
			case REPLY:
				val = statusHT->read(to_string(mesg->transID));
				pos = val.find("::");
				start = 0;
				while (pos != string::npos) {
					string field = val.substr(start, pos-start);
					tuple.push_back(field);
					start = pos + 2;
					pos = val.find("::", start);
				}
				tuple.push_back(val.substr(start));
				successCount = stoi(tuple.at(1));
				failureCount = stoi(tuple.at(2));

				if(mesg->success)
					successCount = successCount + 1;
				else
					failureCount = failureCount + 1;

				statusHT->update(to_string(mesg->transID), tuple.at(0) + "::" + to_string(successCount) + "::" + to_string(failureCount)+ "::" + tuple.at(3) + "::" + tuple.at(4));
				if(tuple.at(0) == "CREATE"){
					if(successCount == 2)
						log->logCreateSuccess(&(memberNode->addr), true, mesg->transID, tuple.at(3), tuple.at(4));
					else if(failureCount == 2)
						log->logCreateFail(&(memberNode->addr), true, mesg->transID, tuple.at(3), tuple.at(4));
				}
				else if(tuple.at(0) == "DELETE"){
					if(successCount == 2)
						log->logDeleteSuccess(&(memberNode->addr), true, mesg->transID, tuple.at(3));
					else if(failureCount == 2)
						log->logDeleteFail(&(memberNode->addr), true, mesg->transID, tuple.at(3));
				}

				break;

			case READREPLY:
				val = statusHT->read(to_string(mesg->transID));
				pos = val.find("::");
				start = 0;
				while (pos != string::npos) {
					string field = val.substr(start, pos-start);
					tuple.push_back(field);
					start = pos + 2;
					pos = val.find("::", start);
				}
				tuple.push_back(val.substr(start));
				successCount = stoi(tuple.at(1));
				failureCount = stoi(tuple.at(2));

				if(!((mesg->value).empty()))
					successCount = successCount + 1;
				else
					failureCount = failureCount + 1;

				statusHT->update(to_string(mesg->transID), tuple.at(0) + "::" + to_string(successCount) + "::" + to_string(failureCount)+ "::" + tuple.at(3) + "::" + tuple.at(4));

				if(successCount == 2){
					Entry *en = new Entry(mesg->value);
					log->logReadSuccess(&(memberNode->addr), true, mesg->transID, tuple.at(3), en->value);
				}
				else if(failureCount == 2)
					log->logReadFail(&(memberNode->addr), true, mesg->transID, tuple.at(3));

				break;
		}
		/*
		 * Handle the message types here
		 */

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	map<string, string> hash = ht->hashTable;
	string key,value;
	for(map<string,string>::iterator it = hash.begin(); it != hash.end(); it++) {
	    key = it->first;
	    value = it->second;
	    Entry *en = new Entry(value);

	    vector<Node> replicas = findNodes(key);



	    switch(en->replica){
	    case PRIMARY:


	    	break;
	    case SECONDARY:
	    	break;
	    case TERTIARY:
	    	break;
	    }
	}
}

void MP2Node:: findNeighbors(){
	for (int i=0; i<ring.size(); i++){
		Node addr = ring.at(i);
		if((addr.nodeAddress) == (memberNode->addr))
		{
			hasMyReplicas.clear();
			hasMyReplicas.push_back(ring.at((i+1)%ring.size()));
			hasMyReplicas.push_back(ring.at((i+2)%ring.size()));
			haveReplicasOf.clear();
			haveReplicasOf.push_back(ring.at((i-1)%ring.size()));
			haveReplicasOf.push_back(ring.at((i-2)%ring.size()));
			break;
		}

	}
}
