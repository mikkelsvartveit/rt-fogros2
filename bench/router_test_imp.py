import requests
from enum import Enum

class RouterRef:
    def __init__(self, addr : str, gdp_name : str):
        self.address = addr
        self.gdp_name = gdp_name
        
    def __repr__(self):
        return self.address + self.gdp_name
    
    def __hash__(self):
        return hash(repr(self))
        
class RIB:
    def __init__(self):
        self.children = {} #mapping from name to address of children
        self.members = {} #mapping from name to address of members
        self.tentative_links = {} #tentative multicast links
        self.confirmed_links = {} #confirmed multicast links
        self.mg_members = {}
       
class MessageType(Enum):
    CREATE_TD = "CREATE_TD"
    JOIN_TD = "JOIN_TD"
    LEAVE_TD = "LEAVE_TD"
    CREATE_MG = "CREATE_MG"
    JOIN_MG = "JOIN_MG"
    LEAVE_MG = "LEAVE_MG"
    SEND_MG = "SEND_MG"
    KEY_ROTATE_MG = "KEY_ROTATE_MG"       
        
class Message:
    def __init__(self, msg_type : str):
        self.msg_type = msg_type
        self.payload = {}

class Router:
    def __init__(self, addr : str, gdp_name : str, parent : RouterRef):
        self.curr_router = RouterRef(addr, gdp_name)
        self.parent_router = parent
        self.parent_confirmed = False
        self.rib = RIB()
        self.message_queue = []
        self.message_handlers = {}
        self.setup_handlers()
    
    """
    Each handler returns an destination for sending its output, and the payload of the output itself.
    """
    def create_td_handler(self, msg : Message):
        child_addr, child_gdp_name = msg.payload["addr"], msg.payload["gdp_name"]
        self.rib.children[child_gdp_name] = child_addr
        return [("", {})]
    
    def join_td_handler(self, msg : Message):
        member_addr, member_gdp_name = msg.payload["addr"], msg.payload["gdp_name"]
        intent = msg.payload["intent"]
        if intent == "member":
            self.rib.members[member_gdp_name] = member_addr
        else:
            self.rib.children[member_gdp_name] = member_addr
        return [("", {})]
    
    def leave_td_handler(self, msg : Message):
        member_gdp_name = msg.payload["gdp_name"]
        if member_gdp_name in self.rib.members:
            self.rib.members.pop(member_gdp_name)
        elif member_gdp_name in self.rib.children:
            self.rib.children.pop(member_gdp_name)
        return [("", {})]
    
    """
    Multicast group handlers
    """
    def create_mg_handler(self, msg : Message):
        #TODO: CRYPTO -- issue certs
        child_addr, child_gdp_name = msg.payload["addr"], msg.payload["gdp_name"]
        mg_gdp_name = msg.payload["mg_gdp_name"]
        
        if child_gdp_name in self.rib.children: #if from a child        
            #create a tentative link
            if mg_gdp_name not in self.rib.tentative_links:
                self.rib.tentative_links[mg_gdp_name] = set()
            self.rib.tentative_links[mg_gdp_name].add(RouterRef(child_addr, child_gdp_name))
        else: #if from a member
            #add to interest set for the multicast group
            if mg_gdp_name not in self.rib.mg_members:
                self.rib.mg_members[mg_gdp_name] = set()
            self.rib.mg_members[mg_gdp_name].add(RouterRef(child_addr, child_gdp_name))
            
        #propagate a tentative link up the tree
        payload_up = {
            "addr" : self.curr_router.address,
            "gdp_name" : self.curr_router.gdp_name,
            "mg_gdp_name" : mg_gdp_name
        }
        
        return [(self.parent_router.address, payload_up)]
            
    
    def join_mg_handler(self, msg : Message):
        child_addr, child_gdp_name = msg.payload["addr"], msg.payload["gdp_name"]
        mg_gdp_name = msg.payload["mg_gdp_name"]
        direction = msg.payload["direction"]
        
        if child_gdp_name in self.rib.children: #if from a child        
            if direction == "up":
                tentative_set, confirmed_set = self.rib.tentative_links[mg_gdp_name], self.rib.confirmed_links[mg_gdp_name]
                size_before = len(tentative_set) + len(confirmed_set)
                
                #create a tentative link
                if mg_gdp_name not in self.rib.tentative_links:
                    self.rib.tentative_links[mg_gdp_name] = set()
                self.rib.tentative_links[mg_gdp_name].add(RouterRef(child_addr, child_gdp_name))
                    
                #if there was already a tentative or confirmed link on this node for this multicast group, we have found the lowest common ancestor and need to change directions
                if size_before > 0:
                    messages = []
                    
                    #convert all tentative children to confirmed and propagate down
                    tentative_set = self.rib.tentative_links[mg_gdp_name]
                    while tentative_set: #guaranteed to exist -- if tentative link before, exists. if not, we created one before
                        tentative_child = tentative_set.pop()
                        self.rib.confirmed_links[mg_gdp_name].add(tentative_child)
                        messages.append((tentative_child.address, {"direction" : "down", 
                                                                   "mg_gdp_name" : mg_gdp_name, 
                                                                   "addr" : "", 
                                                                   "gdp_name" : ""}))
                    self.rib.tentative_links.pop(mg_gdp_name) #get rid of the set, it is empty
                    
                    return messages
                    
                #if we didn't find the lowest common ancestor, propagate up the tree
                payload_up = {
                    "addr" : self.curr_router.address,
                    "gdp_name" : self.curr_router.gdp_name,
                    "direction" : "up",
                    "mg_gdp_name" : mg_gdp_name
                }
                return [(self.parent_router.address, payload_up)]
            else: #going down
                #turn all tentative links for this group into confirmed links
                messages = []
                    
                #convert all tentative children to confirmed and propagate down
                self.parent_confirmed = True
                tentative_set = self.rib.tentative_links.get(mg_gdp_name, set())
                while tentative_set:
                    tentative_child = tentative_set.pop()
                    self.rib.confirmed_links[mg_gdp_name].add(tentative_child)
                    messages.append((tentative_child.address, {"direction" : "down"}))
                self.rib.tentative_links.pop(mg_gdp_name) #get rid of the set, it is empty
                
                return messages
        else: #if from a member
            #add to interest set for the multicast group
            if mg_gdp_name not in self.rib.mg_members:
                self.rib.mg_members[mg_gdp_name] = set()
            self.rib.mg_members[mg_gdp_name].add(RouterRef(child_addr, child_gdp_name))
            
            #if from a member, propagate up
            payload_up = {
                "addr" : self.curr_router.address,
                "gdp_name" : self.curr_router.gdp_name,
                "direction" : "up",
                "mg_gdp_name" : mg_gdp_name
            }
            return [(self.parent_router.address, payload_up)]
            
    def send_mg_handler(self, msg : Message):
        mg_gdp_name = msg.payload["mg_gdp_name"]
        content = msg.payload["content"]
        source_addr, source_gdp_name = msg.payload["addr"], msg.payload["gdp_name"]
        
        #send to direct members
        direct_members = self.rib.mg_members.get(mg_gdp_name, [])
        messages = []
        
        curr_msg = {
            "addr" : self.curr_router.address,
            "gdp_name" : self.curr_router.gdp_name,
            "mg_gdp_name" : mg_gdp_name,
            "content" : content  
        }
        for direct_member in direct_members:
            messages.append((direct_member.address, curr_msg))
        
        #send to parent and confirmed children
        if self.parent_confirmed:
            messages.append((self.parent_router.address, curr_msg))
        
        for conf_child in self.rib.confirmed_links.get(mg_gdp_name, set()):
            messages.append((conf_child.address, curr_msg))
        
        return messages       
    
    def leave_mg_handler(self, msg : Message): #assume this is more rare, unoptimized
        #TODO: Implement
        return
    
    def key_rotate_mg_handler(self, msg : Message):
        #TODO: CRYPTO
        return
        
    def setup_handlers(self):
        self.handlers[MessageType.CREATE_TD.value] = self.create_td_handler
        self.handlers[MessageType.JOIN_TD.value] = self.join_td_handler
        self.handlers[MessageType.LEAVE_TD.value] = self.leave_td_handler
        self.handlers[MessageType.CREATE_MG.value] = self.create_mg_handler
        self.handlers[MessageType.JOIN_MG.value] = self.join_mg_handler
        self.handlers[MessageType.LEAVE_MG.value] = self.leave_mg_handler
        self.handlers[MessageType.SEND_MG.value] = self.send_mg_handler
        self.handlers[MessageType.KEY_ROTATE_MG.value] = self.key_rotate_mg_handler
        
    def handle_message(self, msg : Message): #WARNING: THIS SERIALIZES ALL HANDLERS
        handler = self.message_handlers.get(msg.msg_type, None)
        if not handler:
            return
        return handler(msg)
        
    def process_messages(self):
        while True:
            if len(self.message_queue) > 0:
                curr_msg = self.message_queue.pop()
                self.handle_message(curr_msg)

    
    
    
    