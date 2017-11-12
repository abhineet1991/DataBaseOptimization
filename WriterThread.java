import java.io.BufferedOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

public class WriterThread extends Thread {

	//Global Variables
	HashMap<Integer,String> valueHash;
	int age;

	public WriterThread(HashMap _valueHash, int age,String name)
	{
		super(name);
		//Initialization of variables
		this.valueHash = _valueHash;
		this.age = age;
	}


	public void run()
	{
		try { 
			//Loop through the predefined age (given from the project description)

			if(valueHash.containsKey(age)){
				File f = new File("C:\\Users\\dell\\"+age+".index");

				//If index files does not exists, then create it.
				if(!f.exists())
					f.createNewFile();

				//Get the file pointer of age from the HashMap.
				String s = valueHash.get(age).toString();

				//Create Output Stream as BufferedOutPutStream in APPEND Mode and write at the end of File.
				OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(Paths.get("C:\\Users\\Indexes\\"+age+".index"), StandardOpenOption.CREATE,	StandardOpenOption.APPEND));

				//Convert the value into respective byte array.
				byte[] ss = s.getBytes();

				//Write to File starting from 0 index of byte array to the length of byte array (FULL WRITE)
				outputStream.write(ss, 0, ss.length);

				//Flush the OutputStream and write any remaining to the file before we close the stream. 
				outputStream.flush();

				//Close the Stream and release any resources to free up memory - Write operation is completed. 
				outputStream.close();
			}
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}

	}

}
