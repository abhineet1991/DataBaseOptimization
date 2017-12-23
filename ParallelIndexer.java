import java.io.File;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public class ParallelIndexer implements Runnable {

	BlockingQueue<byte[]> dataQueue;
	HashMap<Integer, Integer> hashmap;
	byte[] dataHolder;

	public ParallelIndexer(BlockingQueue<byte[]> e,
			HashMap<Integer, Integer> _hash, int byteSize) {
		this.dataQueue = e;
		this.hashmap = _hash;
		dataHolder = new byte[byteSize];
	}

	public void run() {

		while (!dataQueue.isEmpty()) {
			synchronized (hashmap) {
				String k = "";
				try {
					k = new String(dataQueue.peek(), "UTF-8");
				} catch (UnsupportedEncodingException e1) {
					e1.printStackTrace();
				}
				// int age = Integer.parseInt(k.substring(39, 41));
				// System.out.println(age);
				try {

					System.out.println(new String(dataQueue.take(), "UTF-8"));
					System.out.println(dataQueue.size());

				} catch (Exception e) {

				}

			}
		}

	}

	public static void insert(File filename, long offset, byte[] content) {
		try {

			RandomAccessFile r = new RandomAccessFile(filename, "rw");
			RandomAccessFile rtemp = new RandomAccessFile(new File(
					filename.getName() + "~"), "rw");
			long fileSize = r.length();
			FileChannel sourceChannel = r.getChannel();
			FileChannel targetChannel = rtemp.getChannel();
			sourceChannel
					.transferTo(offset, (fileSize - offset), targetChannel);
			sourceChannel.truncate(offset);
			r.seek(offset);
			r.write(content);
			long newOffset = r.getFilePointer();
			targetChannel.position(0L);
			sourceChannel.transferFrom(targetChannel, newOffset,
					(fileSize - offset));
			sourceChannel.close();
			targetChannel.close();
			Filer.GetMemory();

		} catch (Exception e) {

		}
	}

}
