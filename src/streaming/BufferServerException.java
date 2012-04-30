package streaming;


public abstract class BufferServerException extends Exception{

	private Code code;

	public BufferServerException(Code code){
		this.code=code;
	}
	
	public enum Code{
		EXCEEDED_MAX_CAPACITY,
		LOGBUFFER_OVERFLLOW
	}
	
	public static class BufferMaxCapacityException extends BufferServerException{
		public BufferMaxCapacityException(){
			super(Code.EXCEEDED_MAX_CAPACITY);		
		}
		
	}
	
	public static class BufferOverflowException extends BufferServerException{
		public BufferOverflowException(){
			super(Code.LOGBUFFER_OVERFLLOW);		
		}
		
	}
}
