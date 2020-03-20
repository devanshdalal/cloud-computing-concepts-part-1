/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include <unordered_set>
#include <unordered_map>
#include <map>
#include <cstdlib>
#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5
#define SWIMK 2
#define DETECTION_INTERVAL 30 // in globaltime units
#define WAITING_TIMEOUT 10	// in globaltime units

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

// 1. node add, leave,
// 2. SWIM failure detection
// <PING> <addr>
// <FAILED> <addr>
/**
 * Message Types
 */
enum MsgTypes
{
	JOINREQ, // <node> <dest>
	JOINREP, // <list of all nodes in the system>
	FAILED,  // <failed node> <publisher>
	PING,	// <src> <dest>
	PONG,	// <src> <dest>
	DUMMYLASTMSGTYPE
};

enum NodeState
{
	TIMEOUTWAITING,
	SWIMWAITING,
	FINALWAITING,
};

struct NodeStatus
{
	enum NodeState state;
	long timeout;
	int node; // waiting node, -1 otherwise
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr
{
	enum MsgTypes msgType;
} MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node
{
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];

	// extra defined values
	std::unordered_set<int> m_;
	std::unordered_map<int, NodeStatus> local_state_; // waiting node to its status map
	int last_detection_ = 0;
	int this_node_;

	void PingK(int node_id);
	void ModifyLocalState();
	void PublishToAll(const MessageHdr &msg, size_t size);
	void AddNode(int node);

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member *getMemberNode()
	{
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
