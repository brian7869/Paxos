 - Instructions on how to use it:
 	
 	0. Edit configuration in config.py if necessary

 	1A. For local testing:

 		Decide these following input arguments before continuing 
	 		a. max_failure: That is f (Default maximum: 7)
		 	b. # of clients: Number of clients you want to test (Default Maximum: 30)
		 	c. # of commands: Number of commands each client will send to servers
		 	d. can_skip_slot: Max number of slots one server can skip when it is in power
		 	e. fail_view_change: Deliberately fail on view change for f times

 		Run: $ python local_test.py <max_failure> <# of clients> <# of commands> <can_skip_slot> <fail_view_change>

	1B. For testing on multiple machines:

		*** Set server_addresses and client_addresses to correct values in step 0 before running!!! *** 

		Run $ python start_server.py <max_failure> <replica_id> <can_skip_slot> <fail_view_change>

		Run $ python start_client.py <max_failure> <client_id> <num_commands>

 	2. To check chat_log, see 'server_<replica_id>.chat_log' under chat_log/

 	3. Kill all python processes!

 - How we implement it:

 	1. Failure detection: 
 		a. Heartbeat: Every replica keeps a live set using heartbeat. Replica will braodcast "IAmLeader" message to all replicas 
 						if it thinks it is the one with smallest replica_id	in its live set.
 		b. Client timeout: When client timeouts on current leader, it will broadcast "ViewChange" message to all replicas.
 							Upon receiving "ViewChange", replicas will check its live set and will campaign for the leader 
 							if it is the one with smallest replica_id in its live set.
 	2. 