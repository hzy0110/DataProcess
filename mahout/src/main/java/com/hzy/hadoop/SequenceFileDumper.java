/**
 *
 */
package com.hzy.hadoop;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.hzy.util.HUtils;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import java.io.Closeable;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用方式：
 * 1. 实例化；
 * 2. 运行run方法；
 * 3. 使用getRetStr获得读取的信息值
 * @author fansy
 * @date 2015年8月10日
 */
public final class SequenceFileDumper extends AbstractJob {

	private String retStr = null;

	public SequenceFileDumper() {
		setConf(HUtils.getConf());
	}

	@Override
	public int run(String[] args) throws Exception {

		addInputOption();
		addOutputOption();
		addOption("splitter", "sp", " the return string splitter ", false);
		addOption("substring", "b", "The number of chars to print out per value", false);
		addOption(buildOption("count", "c", "Report the count only", false, false, null));
		addOption("numItems", "n", "Output at most <n> key value pairs", false);
		addOption(buildOption("facets", "fa", "Output the counts per key.  Note, if there are a lot of unique keys, this can take up a fair amount of memory", false, false, null));
		addOption(buildOption("quiet", "q", "Print only file contents.", false, false, null));

		if (parseArguments(args, false, true) == null) {
			return -1;
		}


		Path[] pathArr;
		Configuration conf = getConf();
		Path input = getInputPath();
		FileSystem fs = input.getFileSystem(conf);
		if (fs.getFileStatus(input).isDir()) {
			pathArr = FileUtil.stat2Paths(fs.listStatus(input, PathFilters.logsCRCFilter()));
		} else {
			pathArr = new Path[1];
			pathArr[0] = input;
		}


//	    Writer writer;
//	    boolean shouldClose;
		StringBuffer buff;
		buff = new StringBuffer();
		String splitter = null;
		if (hasOption("splitter")) {
			splitter = getOption("splitter");
		}
		Object writer;
		boolean shouldClose;
		if (this.hasOption("output")) {
			shouldClose = true;
			writer = Files.newWriter(new File(this.getOption("output")), Charsets.UTF_8);
		} else {
			shouldClose = false;
			writer = new OutputStreamWriter(System.out, Charsets.UTF_8);
		}
		try {
			for (Path path : pathArr) {
				if (!hasOption("quiet")) {
					buff.append("Input Path: ").append(String.valueOf(path)).append(splitter);
				}

				int sub = Integer.MAX_VALUE;
				if (hasOption("substring")) {
					sub = Integer.parseInt(getOption("substring"));
				}
				boolean countOnly = hasOption("count");
				SequenceFileIterator<?, ?> iterator = new SequenceFileIterator<>(path, true, conf);
				if (!hasOption("quiet")) {
					buff.append("Key class: ").append(iterator.getKeyClass().toString());
					buff.append(" Value Class: ").append(iterator.getValueClass().toString()).append(splitter);
				}
				OpenObjectIntHashMap<String> facets = null;
				if (hasOption("facets")) {
					facets = new OpenObjectIntHashMap<>();
				}
				long count = 0;
				if (countOnly) {
					while (iterator.hasNext()) {
						Pair<?, ?> record = iterator.next();
						String key = record.getFirst().toString();
						if (facets != null) {
							facets.adjustOrPutValue(key, 1, 1); //either insert or add 1
						}
						count++;
					}
					buff.append("Count: ").append(String.valueOf(count)).append(splitter);
				} else {
					long numItems = Long.MAX_VALUE;
					if (hasOption("numItems")) {
						numItems = Long.parseLong(getOption("numItems"));
						if (!hasOption("quiet")) {
							buff.append("Max Items to dump: ").append(String.valueOf(numItems)).append(splitter);
						}
					}
					while (iterator.hasNext() && count < numItems) {
						Pair<?, ?> record = iterator.next();
						String key = record.getFirst().toString();
						buff.append("Key: ").append(key);
						String str = record.getSecond().toString();
						buff.append(": Value: ").append(str.length() > sub
								? str.substring(0, sub) : str);
						buff.append(splitter);
						if (facets != null) {
							facets.adjustOrPutValue(key, 1, 1); //either insert or add 1
						}
						count++;
					}
					if (!hasOption("quiet")) {
						buff.append("Count: ").append(String.valueOf(count)).append(splitter);
					}
				}
				if (facets != null) {
					List<String> keyList = new ArrayList<>(facets.size());

					IntArrayList valueList = new IntArrayList(facets.size());
					facets.pairsSortedByKey(keyList, valueList);
					buff.append("-----Facets---\n");
					buff.append("Key\t\tCount\n");
					int i = 0;
					for (String key : keyList) {
						buff.append(key).append("\t\t").append(String.valueOf(valueList.get(i++))).append(splitter);
					}
				}
			}
//	      writer.flush();

		} finally {
			System.out.println("SequenceFileDumper finished!");
			retStr = buff.toString();
			((Writer)writer).append(retStr);
			if(shouldClose) {
				Closeables.close((Closeable) writer, false);
			}
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		//new org.apache.mahout.utils.SequenceFileDumper().run(args);
		new SequenceFileDumper().run(args);
	}

	/**
	 * @return the retStr
	 */
	public String getRetStr() {
		return retStr;
	}

	/**
	 * @param retStr the retStr to set
	 */
	public void setRetStr(String retStr) {
		this.retStr = retStr;
	}

}

