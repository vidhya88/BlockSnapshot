package edu.cornell.cs.weijia.bs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.tests.CorfuHello;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cornell.cs.weijia.bs.MemBlock.BlockImplement;

public class CORFUSharedLog {
	String masteraddress = "http://localhost:8002/corfu";
	private static final Logger log = LoggerFactory.getLogger(CorfuHello.class);
	final int numthreads=1;
	CorfuDBClient client;
	Sequencer seq;
	WriteOnceAddressSpace woas;
	SharedLog shared_log;
	static ConcurrentHashMap<String, MemBlock> memblock_map=new ConcurrentHashMap<String, MemBlock>();
	
public enum Command{
		CREATE_SS(1),DELETE_SS(2),WRITE(3),CREATE_MEM_BLOCK(4),DELETE_MEM_BLOCK(5);
		private int numVal;
		Command(int numVal) {
	        this.numVal = numVal;
	    }

	    public int getNumVal() {
	        return numVal;
	    }
	}
	
	abstract class CommandType 
	{
		//String cmd_blockID;				
		abstract byte[] getBytes();		
		abstract  Object getObject(byte[] objb);		
	}

	class createSS_deleteSS_commandType extends CommandType // for create and delete snapshot
	{
	    Object sid;
	   
	    
	    byte[] getBytes()
	    {
	    	byte[] sid_b=null;
	    	try {
				sid_b=Convert_to_bytearray();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	return sid_b;
	    }
	    
	    public createSS_deleteSS_commandType(String pcmd,Object psid,String pblockUUID)
	    {
	    	sid=psid;
	    //	cmd_blockID=pcmd + pblockUUID;
	    	
	    	
	    }
	    public createSS_deleteSS_commandType()
	    {
	    	
	    }
	    public Object getObject(byte[] objb)
	    {
	    	Object obj=null;
	    	  try {
				obj=Convert_bytes_to_object(objb);
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	  return obj;
	    }
	   
	    byte[]  Convert_to_bytearray() throws IOException
	    {	    	
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    ObjectOutput out = null;
	    byte[] sid_b=null;
	    try {
	      out = new ObjectOutputStream(bos);   
	      out.writeObject(sid);
	      sid_b = bos.toByteArray();
	    } finally {
	      try {
	        if (out != null) {
	          out.close();
	        }
	      } catch (IOException ex) {
	        
	      }
	      try {
	        bos.close();
	      } catch (IOException ex) {
	    
	      }
	    }
	    return sid_b;
	}
	    
	    Object Convert_bytes_to_object(byte[] sid_b) throws IOException, ClassNotFoundException
	    {
	    	ByteArrayInputStream bis = new ByteArrayInputStream(sid_b);
	    	ObjectInput in = null;
	    	Object obj;
	    	try {
	    		in = new ObjectInputStream( bis);
	    		obj = in.readObject(); 
	      
	    	} finally {
	    		try {
	    			bis.close();
	    		} catch (IOException ex) {
	        // ignore close exception
	    		}
	    		try {
	    			if (in != null) {
	    				in.close();
	    			}
	    		} catch (IOException ex) {
	    			// ignore close exception
	    		}
	    	}
	    	return obj;
	    }
	}

	class write_commandType extends CommandType //write
	{
	    String offset;
	    String length;
	    byte[] buf; 
	 
	    public write_commandType(String pcmd,int poffset,int plength,byte[] pbuf,String pblockID)
	    {
	   // 	cmd_blockID=pcmd+pblockID;
	    	offset=String.valueOf(poffset);
	    	length=String.valueOf(plength);
	    	buf=pbuf;
	    	
	    }
	    public write_commandType()
	    {
	  //  	cmd_blockID="";
	    	offset="0";
	    	length="0";
	    	buf=new byte[100];
	    	
	    }
	   
	    byte[] getBytes()
	    {
	    	  byte[] objb;
			  objb= new byte[offset.getBytes().length +":".getBytes().length+ length.getBytes().length+ ":".getBytes().length+buf.length];
			  System.arraycopy((offset+":").getBytes(), 0, objb, 0, (offset+":").getBytes().length);
			  System.arraycopy((length+":").getBytes(), 0, objb, (offset+":").getBytes().length, (length+":").getBytes().length);
			  System.arraycopy(buf,0, objb, (offset+":").getBytes().length+ (length+":").getBytes().length, buf.length);
			  return objb;
			  
			
	    }
	    
	    
	    @Override
		Object getObject(byte[] objb) {
			// TODO Auto-generated method stub
			return null;
		}
	    
	      
	}
	
	//for create memblocks
	class createMemblk_commandType extends CommandType{
		String mem_block_type;
		
		public createMemblk_commandType(String pcmd,String p_mem_block_type,String pblockID)
	    {
	    	mem_block_type=p_mem_block_type;
	    	//cmd_blockID=pcmd+pblockID;
	    	
	    }
		@Override
		byte[] getBytes() {
			// TODO Auto-generated method stub
			return (":"+mem_block_type).getBytes();
		}
		@Override
		Object getObject(byte[] objb) {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	public CORFUSharedLog()
	{
	      client = new CorfuDBClient(masteraddress);
	      seq = new Sequencer(client);
	      woas = new WriteOnceAddressSpace(client);
	      shared_log = new SharedLog(seq, woas);
	      client.startViewManager();
	}
	
	public void playback_log()
	{
		int i=0;
		// = MemBlock.createMemBlock(BlockImplement.PAGE_COW_HEAP);
		MemBlock.restore_mode=true;
		while(true)
		{
			try 
			  {
				log.info("Reading back entry at address " + i);
				byte[] result,cmd_result,param_result,blockid;
				result = shared_log.read(i++);
				cmd_result=Arrays.copyOfRange(result, 0, 1);
				blockid=Arrays.copyOfRange(result, 1,37);
				param_result=Arrays.copyOfRange(result, 37,result.length);			
				log.info("Readback complete, cmd size=" + cmd_result.length);
				log.info("Readbacfk complete, blockid size=" + blockid.length);;
				log.info("Readback complete, param size=" + param_result.length);
			    String sresult = new String(cmd_result, "UTF-8");		    
			    log.info("Contents were: " + sresult);	
			    
			    if(sresult.startsWith(String.valueOf(Command.CREATE_MEM_BLOCK.getNumVal()))){
			    	String[] temp=(new String (param_result,"UTF-8")).split(":");	
			    	log.info("CREATE Block CMD "+new String(param_result,"UTF-8"));
			    	//get the UUID and enter it in the Memblock Array and create a new Memblock object
			    	if(!memblock_map.containsKey(blockid.toString())){
			    		MemBlock b= MemBlock.createMemBlock(BlockImplement.valueOf(temp[1]),new String(blockid,"UTF-8"));
			    		memblock_map.put(blockid.toString(), b);
			    	}
			    	
			    }
			    else if(sresult.startsWith(String.valueOf(Command.CREATE_SS.getNumVal())) || sresult.startsWith(String.valueOf(Command.DELETE_SS.getNumVal()))){
			    	CommandType param=new createSS_deleteSS_commandType();
			    	Object sid=param.getObject(param_result);				    	
			    	if(sresult.equals(String.valueOf(Command.CREATE_SS.getNumVal()))){
			    		log.info("CREATE CMD "+new String(param_result,"UTF-8"));
			    		MemBlock b= memblock_map.get(new String(blockid,"UTF-8"));
			    		if(b!=null)
			    			b.createSnapshot(sid);
			    	}
			    	else{
			    		log.info("DELETE CMD "+new String(param_result,"UTF-8"));
			    		MemBlock b= memblock_map.get(new String(blockid,"UTF-8"));
			    		if(b!= null)
			    			b.deleteSnapshot(sid);
			    	}
			    }
			    else{
			    
			    	String[] temp=(new String (param_result,"UTF-8")).split(":");	
			    	log.info("WRITE COMMAND "+new String (param_result,"UTF-8"));
			    	MemBlock b= memblock_map.get(new String(blockid,"UTF-8"));
			    	if(b!=null)
			    		b.write(Integer.parseInt(temp[0]),Integer.parseInt(temp[1]), temp[2].getBytes());
			    }			    	    
			      
			} catch (UnwrittenException e) {
			
				break;
			}
			catch (Exception e){
				
				break;
				
			}
			
				
			
		}
		MemBlock.restore_mode=false;
	}
	
	public boolean writeToLog(String cmd, Object sid, String pblockID)
	{
		if(writeToLog(cmd+pblockID, new createSS_deleteSS_commandType(cmd,sid,pblockID)))
			return true;
		else
			return false;
	}
	public boolean writeToLog(String cmd, String mem_block_type, String pblockID){
		if(writeToLog(cmd+pblockID, new createMemblk_commandType(cmd,mem_block_type,pblockID)))
			return true;
		else
			return false;
	}
	public boolean writeToLog(String cmd,int offset,int length,byte[] buf,String pblockID )
	{
		if(writeToLog(cmd+pblockID, new write_commandType(cmd,offset,length,buf,pblockID)))
			return true;
		else
			return false;
	}
	
	public boolean writeToLog(String cmd,CommandType param) 
	  { 		
			long cmd_address=0,param_address=0;			
			// create a destination array that is the size of the two arrays
			byte[] destination = new byte[cmd.getBytes().length+param.getBytes().length];	
		   	System.arraycopy(cmd.getBytes(), 0, destination, 0, cmd.getBytes().length); 			
			System.arraycopy(param.getBytes(), 0, destination, cmd.getBytes().length, param.getBytes().length);			
			cmd_address = shared_log.append(destination);				 
			log.info("Successfully appended "+cmd+" into log position " + cmd_address);		 
			return true;
	      
	  }
	
}
