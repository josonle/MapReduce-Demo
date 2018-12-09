package ssdut.training.mapreduce.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class StartRun {
	public static void main(String[] args) throws IllegalArgumentException, ClassNotFoundException, IOException, InterruptedException {
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		Map<String, String> paths = new HashMap<String, String>();
		paths.put("Step1Input", "/expr/itemcf/data");
		paths.put("Step1Output", "/expr/itemcf/output/output1");
		
		paths.put("Step2Input", paths.get("Step1Output"));	//后面每一步的输入路径都是前一步的输出路径
		paths.put("Step2Output", "/expr/itemcf/output/output2");
		
		paths.put("Step3Input", paths.get("Step2Output"));
		paths.put("Step3Output", "/expr/itemcf/output/output3");
		
		paths.put("Step4Input1", paths.get("Step2Output"));
		paths.put("Step4Input2", paths.get("Step3Output"));
		paths.put("Step4Output", "/expr/itemcf/output/output4");
		
		paths.put("Step5Input", paths.get("Step4Output"));
		paths.put("Step5Output", "/expr/itemcf/output/output5");
		
		paths.put("Step6Input", paths.get("Step5Output"));
		paths.put("Step6Output", "/expr/itemcf/output/output6");
		
		Step1.run(conf, paths);	//去重
		Step2.run(conf, paths);	//计算用户评分矩阵
		Step3.run(conf, paths);	//计算同现矩阵
		Step4.run(conf, paths);	//计算单项评分=同现矩阵*评分矩阵
		Step5.run(conf, paths);	//计算评分总和
		Step6.run(conf, paths);	//评分排序取Top10
		
		System.out.println("finished!");
	}

	public static Map<String, Integer> R = new HashMap<String, Integer>();
	static {
		R.put("click", 1);		//浏览
		R.put("collect", 2);	//收藏
		R.put("cart", 3);		//放入购物车
		R.put("alipay", 4);		//支付
	}
}
