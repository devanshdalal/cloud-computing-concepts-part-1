/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <thread>
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */

namespace
{
constexpr size_t MSGSIZE = sizeof(MessageHdr) + sizeof(int) + sizeof(int);
MessageHdr *CreateMessage(MsgTypes type, int src, int target)
{
    MessageHdr *msg = (MessageHdr *)malloc(MSGSIZE * sizeof(char));
    msg->msgType = type;
    memcpy((char *)(msg + 1), &src, sizeof(int));
    memcpy((char *)(msg + 1) + sizeof(int), &target, sizeof(int));
    return msg;
}
} // namespace

MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address)
{
    for (int i = 0; i < 6; i++)
    {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->this_node_ = *(int *)address;
    // std::cout << "this_node_: " << this_node_ << "\n";
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop()
{
    if (memberNode->bFailed)
    {
        return false;
    }
    else
    {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size)
{
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport)
{
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1)
    {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr))
    {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr)
{
    /*
	 * This function is partially implemented and may require changes
	 */
    // int id = *(int *)(&memberNode->addr.addr);
    // int port = *(short *)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr)
{
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr)))
    {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        AddNode(this_node_);
    }
    else
    {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        // cout << "msgsize: " << sizeof(MessageHdr) << " " << sizeof(joinaddr->addr) << " " << sizeof(long) << " " << 1 << "\n";
        MessageHdr *msg = (MessageHdr *)malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg + 1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        free(msg);
    }

    return 1;
}

void MP1Node::AddNode(int node)
{
    if (m_.find(node) != m_.end())
        return;
    m_.insert(node);
    memberNode->memberList.emplace_back(node, 0);
    if (node == this_node_)
    {
        memberNode->inGroup = 1;
    }
    Address a(std::to_string(node) + ":0");
#ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, &a);
#endif
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode()
{
    /*
    * Your code goes here
    */
    return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop()
{
    // std::cout << "Start nodeLoop " << this_node_ << "\n";
    if (memberNode->bFailed)
    {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if (!memberNode->inGroup)
    {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    // std::cout << "End nodeLoop\n";
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages()
{
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty())
    {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size)
{
    /*
	 * Your code goes here
	 */
    MessageHdr *msg = (MessageHdr *)data;
    const auto current_time = par->getcurrtime();
    // multicast the membership list.
    vector<MemberListEntry> &memberList = memberNode->memberList;
    switch (msg->msgType)
    {
    case JOINREQ: // this is the introducer.
    {
        AddNode(*(int *)(msg + 1));

        size_t msgsize = sizeof(MessageHdr) + memberList.size() * sizeof(int);
        // cout << "msgsize: " << sizeof(MessageHdr) << " " << sizeof(joinaddr->addr) << " " << sizeof(long) << " " << 1 << "\n";
        MessageHdr *message = (MessageHdr *)malloc(msgsize * sizeof(char));
        message->msgType = JOINREP;
        int *fields = (int *)(message + 1);
        for (int i = 0; i < memberList.size(); ++i)
        {
            fields[i] = memberList[i].getid();
        }
        // multicast to the group.
        PublishToAll(*message, msgsize);
    }
    break;

    case JOINREP:
    {
        std::cout << "JOINREP " << this_node_ << "\n";
        int nodes = (size - sizeof(MessageHdr)) / sizeof(int);
        int *fields = (int *)(msg + 1);
        for (int i = 0; i < nodes; ++i)
        {
            if (m_.find(fields[i]) != m_.end())
                continue;
            AddNode(fields[i]);
        }
    }
    break;

    case PING:
    {
        const int &src = *(int *)(msg + 1);
        const int &dest = *((int *)(msg + 1) + 1);
        std::cout << "PING " << this_node_ << " " << src << " " << dest << "\n";
        if (dest == this_node_)
        {
            // send a PONG to the requesting node.
            auto *message = CreateMessage(PONG, this_node_, src);
            Address a(std::to_string(src) + ":0");
            // memcpy(&a.addr, (char *)(msg + 1), sizeof(a.addr));
            emulNet->ENsend(&memberNode->addr, &a, (char *)message, MSGSIZE); // no need to check status
            // local_state_[src] = {FINALWAITING, current_time + WAITING_TIMEOUT, -1};
            free(msg);
        }
        else
        {
            std::cout << "Indirect PING\n";
            // send a PING to the requested node and later send PONG back to the client to the requesting node.
            auto *message = CreateMessage(PING, this_node_, dest);
            Address a(std::to_string(dest) + ":0");
            emulNet->ENsend(&memberNode->addr, &a, (char *)message, MSGSIZE); // no need to check status
            local_state_[dest] = {FINALWAITING, current_time + WAITING_TIMEOUT, src};
            free(msg);
        }
    }
    break;

    case PONG:
    {
        const int &src = *(int *)(msg + 1);
        const int &dest = *((int *)(msg + 1) + 1);
        std::cout << "PONG " << this_node_ << " " << src << " " << dest << "\n";
        auto it = local_state_.find(src);
        if (it != local_state_.end())
        {
            NodeStatus &status = it->second;
            std::cout << "PONG2 " << status.state << "a\n";
            switch (status.state)
            {
            case TIMEOUTWAITING:
                break;
            case SWIMWAITING:
                break;
            case FINALWAITING:
            {
                std::cout << "FINALWAITING\n";
                auto *message = CreateMessage(PONG, src, status.node);
                Address a(std::to_string(status.node) + ":0");
                emulNet->ENsend(&memberNode->addr, &a, (char *)message, MSGSIZE);
                free(msg);
            }
            break;
            default:
                break;
            }
        }
        local_state_.erase(src);
        for (auto jt = local_state_.begin(); jt != local_state_.end(); ++jt)
        {
            std::cout << "local_stateeee: " << jt->first << " " << jt->second.state << " " << jt->second.timeout << jt->second.node << "\n";
        }
        if (m_.find(src) == m_.end())
        {
            std::cout << "Special case\n";
            // AddNode(src);
        }
    }
    break;

    case FAILED:
    {
        const int &failed = *(int *)(msg + 1);
        std::cout << "FAILED " << failed << " " << this_node_ << " " << (m_.find(failed) != m_.end()) << " " << m_.size() << " " << memberList.size() << "\n";
        if (m_.find(failed) != m_.end())
        {
            m_.erase(failed);
            for (int i = 0; i < memberList.size(); ++i)
            {
                if (memberList[i].getid() == failed)
                {
                    memberList.erase(memberList.begin() + i);
                    break;
                }
            }
            Address a(std::to_string(failed) + ":0");
#ifdef DEBUGLOG
            log->logNodeRemove(&memberNode->addr, &a);
#endif
        }
    }
    break;

    default:
        break;
    }
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps()
{
    std::cout << "Start nodeLoopOps " << this_node_ << " " << last_detection_ << "\n";
    if (!memberNode->memberList.size())
        return;
    const auto current_time = par->getcurrtime();
    if (last_detection_ + DETECTION_INTERVAL > current_time)
    {
        last_detection_ = current_time;
        int x = rand() % memberNode->memberList.size();
        auto &e = memberNode->memberList[x];
        const int &id = e.getid();
        const int &port = e.getport();

        MessageHdr *msg = CreateMessage(PING, this_node_, id);
        Address target_address = std::to_string(id) + ":" + std::to_string(port);

        if (!emulNet->ENsend(&memberNode->addr, &target_address, (char *)msg, MSGSIZE))
        {
            // local_state_[id] = {TIMEOUTWAITING, current_time, -1};
            // failed, so send.
        }
        else
        {
        }
        local_state_[id] = {TIMEOUTWAITING, current_time + WAITING_TIMEOUT, -1};
    }

    ModifyLocalState();

    std::cout << "End nodeLoopOps\n";
    return;
}

void MP1Node::ModifyLocalState()
{
    std::cout << "Start ModifyLocalState\n";
    const auto current_time = par->getcurrtime();
    for (auto it = local_state_.begin(); it != local_state_.end();)
    {
        const auto &node_status = it->second;
        std::cout << "node_status.state: " << node_status.state << " " << this_node_ << " " << it->first << "\n";
        std::cout << "node_status: " << node_status.timeout << " " << current_time << "\n";
        if (node_status.timeout <= current_time)
        {
            switch (node_status.state)
            {
            case FINALWAITING:
            {
                it = local_state_.erase(it);
            }
            break;
            case SWIMWAITING:
            {
                // remove
                auto *message = CreateMessage(FAILED, it->first, this_node_);
                PublishToAll(*message, MSGSIZE);
                free(message);
                it = local_state_.erase(it);
            }
            break;
            case TIMEOUTWAITING:
            {
                PingK(it->first);
                local_state_[it->first] = {SWIMWAITING, current_time + WAITING_TIMEOUT, -1};
            }
            break;
            default:
                break;
            }
            // it = local_state_.erase(it);
        }
        else
        {
            ++it;
        }
    }
    std::cout << "End ModifyLocalState\n";
}

void MP1Node::PublishToAll(const MessageHdr &msg, size_t size)
{
    std::cout << "Start PublishToAll "
              << " " << this_node_ << "," << msg.msgType << "\n";
    for (int i = 0; i < memberNode->memberList.size(); ++i)
    {
        auto &e = memberNode->memberList[i];
        if (e.getid() == this_node_)
            continue;
        Address a(std::to_string(e.getid()) + ":" + std::to_string(e.getport()));
        if (!emulNet->ENsend(&memberNode->addr, &a, (char *)&msg, size))
        {
            // failed to send to this node.
        }
    }
    std::cout << "End PublishToAll\n";
}

void MP1Node::PingK(int node_id)
{
    std::cout << "Start PingK " << this_node_ << " " << node_id << "\n";
    // const auto current_time = par->getcurrtime();
    for (int i = 0; i < SWIMK; ++i)
    {
        int new_x = rand() % memberNode->memberList.size();
        auto &e = memberNode->memberList[new_x];
        const int &new_id = e.getid();
        if (new_id == this_node_ || new_id == node_id)
        {
            --i;
            continue;
        }
        const int &port = e.getport();
        Address a(std::to_string(new_id) + ":" + std::to_string(port));
        MessageHdr *msg = CreateMessage(PING, this_node_, node_id);
        if (!emulNet->ENsend(&memberNode->addr, &a, (char *)msg, MSGSIZE))
        {
            std::cout << "SWIMK ENsend failed\n";
            // failed to send to this node.
        }
        else
        {
            std::cout << "SWIMK ENsend passed" << new_id << "\n";
        }
        free(msg);
    }
    std::cout << "End PingK\n";
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function
 *  checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr)
{
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress()
{
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode)
{
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
           addr->addr[3], *(short *)&addr->addr[4]);
}
