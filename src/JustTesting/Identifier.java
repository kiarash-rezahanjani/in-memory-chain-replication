package JustTesting;

	public  class Identifier
	{
		String client;
		long id;
		public Identifier(String cli, long msgId)
		{client = cli; id = msgId;}
		
		public String getClientId() {
			// TODO Auto-generated method stub
			return client;
		}

		public long getMessageId(){
			return id;
		}

	}