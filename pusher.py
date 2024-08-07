from utils.components.pusher import Push, Server, User, PartsContainer as Parts
import argparse



def main():

	parser = argparse.ArgumentParser(
			description="Caveman Deployments made simple!"
	)
	parser.add_argument('-u','--user', help="dp | ds")
	parser.add_argument('-s','--server', help="eds | dsp | o")
	parser.add_argument('-p', '--parts', help="Comma separated, or ALL, for desired parts (NO SPACES) - eg. jobs,process_handlers would push both Jobs and Process Handlers")
	parser.add_argument('-d','--dryrun', action='store_const', const=1, help="uses dry run mode -- will NOT make actual calls")
	args = parser.parse_args()
	user = None
	server = None
	parts = []

	server = Server(args.server)
	user = User(args.user, server)
	parts = Parts(args.parts)

	push = Push(user, server, parts, args.dryrun)

	push.send()



if __name__ == '__main__':
	main()


