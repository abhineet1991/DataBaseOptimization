import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.HashMap;

/**Class implementing Runnable to become a Thread as well.
 * This Class read the Data File in Chunks of specified size
 * and Store the pointers of the specific index in HashMap via Hashing Function.
 * After reading a block, this class Write the buffered index+pointers
 * to the index file and flush the main memory for next chunk.
 * 
 * @author abhineet.gupta
 * Version 1.1
 */
public class ParallelReader implements Runnable {
	
	int byteSize;								
	//Size of Block in Bytes

	File file;									
	//Reference of input data File

	byte[] myBytes;								
	//Array of bytes to hold the data of input file at each cycle.

	long recordNum=0;							
	//Variable to store total number of records

	int readByte=1000;
	//Byte Size for File Read (Fetching of records)

	HashMap<Integer,String> valueHash = new HashMap<Integer,String>();
	//HashMap to store Index Pointer value based on Age.

//********************************************************************************************************
	
	//Constructor of the Class
	public ParallelReader(File file)
	{

		//Initialization of Global Variables.

		this.file = file;
		myBytes = new byte[byteSize];
	}

//********************************************************************************************************
	
	//Overided method of Runnable that Run at Thread.Start() 
	public void run()
	{
		//StartTime of execution
		long startTime = System.currentTimeMillis();

		//Loop variable based on the fetched Chunk.
		int loopV = 1;

		System.out.println("Starting Index creation....");


		//Read the File as "READ MODE" through RandomAccessFile to Jump at any specific location of File using Seek()
		try (RandomAccessFile data = new RandomAccessFile(file, "r")) 
		{
			//Seek position in Data File. Start is 0;
			int startSeek=0;

			//System.out.println("Length: "+ data.length()+ "|| Array: "+ mulval);
			//Filer.GetMemory();
			//Check for the available Main memory i.e. 2MB or 5MB and set he block size accordingly.
			if((Runtime.getRuntime().freeMemory() / 1024) < 2500)
			{
				byteSize = 4096*(int)(245); //For 2MB - 244 Blocks to read

			} else if((Runtime.getRuntime().freeMemory() / 1024) > 2500)
			{
				byteSize = 4096*(int)(854); //For 5MB - 854 Blocks to read
				//byteSize = 3500000;
				
			}
			//Filer.GetMemory();
			//If the size of data file is less then Block Size then Block Size is equal to Data File length
			//that reads the whole file in one go.
			if(data.length() < byteSize)
			{
				byteSize = (int) file.length();
			}

			myBytes = new byte[byteSize]; //Initialize byte array with calculated byteSize
			//Filer.GetMemory();
			//Calculate the loop iterations based on the chunk size.
			double loopToDo= (double)data.length()/(double)byteSize;
			
			//Total number of records in the input file.
			double numOfRecordsinFile =  (double)data.length()/100;

			if(loopToDo<1)
			{	loopV = 1;
			}	else 
			{
				//Ceil the loop i.e. If loopV is 3.5 then make it run 4 times.
				loopV = (int) Math.ceil(loopToDo);
			}

			readByte = byteSize;
			System.out.println("Total number of Records in the input file: "+String.format("%.0f", numOfRecordsinFile));
			System.out.println("Creating Index files, Please wait.......");
			
			//loop through all records of fetched chunk.
			//iterate through the input file based on the block size till the file is completely read.
			for (long i = 1; i<=loopV; i++) 
			{
				valueHash =  new HashMap<Integer,String>();		//Refresh the HashMap for this Chunk.

				//Calculate available data that is available to be read from the fetched chunk.
				long availableDataToRead = data.length()-(i*byteSize);

				//Read the data into byte Array.
				data.read(myBytes);

				//Move the file read pointer to next value and increment the variables accordingly.
				data.seek(startSeek+byteSize);
				startSeek+=byteSize;

				//Fetch the age from each record and add it to HashMap with the position in data file.
				//Two Variables, lp for loop and P points to the specific byte where Age record
				//in the given 100 byte data is located.
				for(int lp = 0, p = 39; lp<((int)byteSize/100)-1; p+=100, lp++){
					{
						byte[] ageB = {myBytes[p],myBytes[p+1]};
						int age = Integer.parseInt(new String(ageB));
						//System.out.println(age);

						//If the fetched Age is already in the HashMap then Append else add a new value.
						//Append first get the value from the HashMap, modify and write it back.
						if(valueHash.containsKey(age)){
							String tempVal = valueHash.get(age);
							tempVal = tempVal+","+Long.toString(recordNum);
							valueHash.put(age, tempVal);
						} else {
							valueHash.put(age,Long.toString(recordNum));
						}
						
						//Increment the total record number counter.
						recordNum++;
					}
				} //FileStreamClose
				//Filer.GetMemory();

				//For Safe Reading into byteArray - Calculate the available data to read from this Chunk.
				//if Available data is less then byteSize then reAllocate the byteArray to available data length.
				if(availableDataToRead < byteSize)
				{
					byteSize = (int) availableDataToRead;
					
				}
				myBytes = new byte[byteSize];
				//Filer.GetMemory();
				//Write the HashMap values to index file and continue the loop for next block.
				//Necessary to free memory for next block else Memory overflow will occur.
				Filer.writeIndexes(valueHash);
							
			}

			//Filer.GetMemory();
			//Get End Time after reading the whole input file and write functions.
			long endTime2 = System.currentTimeMillis();

			//Total time for index creation. EndTime - StartTime. Divide by 1000 to convert into seconds.
			double indexCreationTime = ((double)(endTime2 - startTime)/(double)1000);
			
			System.out.println("Total # of Disk I/O to create the index file are: " + Filer.fileWriteIO);

			
			System.out.println("Time elapsed to create index files: " + indexCreationTime + " seconds");

			System.out.println("************************************");
			System.out.println("       Index Files Created");
			System.out.println("************************************");

			System.out.println("");
			System.out.println("Please choose the option below: ");
			System.out.println("1 - To Fetch record of an specific age. ");
			System.out.println("2 - To fetch total number of records of the specific age.");
			System.out.println("");

			System.out.println("Type your input:_");
			
			//Get User input to fetch records.
			BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
			int inputOption = Integer.parseInt(input.readLine());
			if(inputOption == 1)
			{
				System.out.println("Please enter the age: ");
				BufferedReader inputAge = new BufferedReader(new InputStreamReader(System.in));
				int inputAgee = Integer.parseInt(inputAge.readLine());

				System.out.println("************************************");
				System.out.println("       Results:");
				System.out.println("************************************");
				
				Filer.readIndexes(inputAgee, readByte, file);

			} else if (inputOption ==2)
			{
				System.out.println("Please enter the age: ");
				BufferedReader inputAge = new BufferedReader(new InputStreamReader(System.in));
				int inputAgee = Integer.parseInt(inputAge.readLine());
				long ouput = Filer.readIndexes(inputAgee,readByte);
				System.out.println("************************************");
				System.out.println("       Results:");
				System.out.println("************************************");
				System.out.println("Total number of records for the age "+inputAgee+" are: "+ouput);

				
			} else {
				System.out.print("Invalid option selected - Program is exiting now! ");
			}

			

			//Read from the index file based on the provided age.
			
			
			endTime2 = System.currentTimeMillis();
			double fetchTime = ((double)(endTime2 - startTime)/(double)1000);

			System.out.println("Total execution time: " + indexCreationTime+fetchTime + " seconds");
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			System.out.println(recordNum);
		}
	}
}
