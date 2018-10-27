package org.talend.designer.codegen.translators.orchestration;

import org.talend.core.model.process.INode;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.IProcess;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import org.talend.core.model.process.ElementParameterParser;

public class TCollectorBeginJava
{
  protected static String nl;
  public static synchronized TCollectorBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCollectorBeginJava result = new TCollectorBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "    \t\t\t";
  protected final String TEXT_2 = NL;
  protected final String TEXT_3 = NL + "    \tlog.info(\"";
  protected final String TEXT_4 = "[\" + Thread.currentThread().getName() + \"] - Start to work.\");" + NL + "    \tlog.info(\"";
  protected final String TEXT_5 = "[\" + Thread.currentThread().getName() + \"] - Use the queue with capability:";
  protected final String TEXT_6 = " to departition data from link:'";
  protected final String TEXT_7 = "'.\");" + NL + "    \t";
  protected final String TEXT_8 = NL + "    \tlog.info(\"";
  protected final String TEXT_9 = "[\" + Thread.currentThread().getName() + \"] - Use ";
  protected final String TEXT_10 = " to collect data into one thread.\");" + NL + "    \t";
  protected final String TEXT_11 = NL + "        StringBuffer log4jSb_";
  protected final String TEXT_12 = " = new StringBuffer();";
  protected final String TEXT_13 = NL + "    resourceMap.put(\"start_";
  protected final String TEXT_14 = "\", true); " + NL + "    synchronized (this) {" + NL + "    \tif (globalMap.get(\"STARTED_";
  protected final String TEXT_15 = "\") == null) {" + NL + "    \t\tclass TargetTask_";
  protected final String TEXT_16 = " extends org.talend.concurrent.ParallelTask {" + NL + "    \t\t\tpublic TargetTask_";
  protected final String TEXT_17 = "(java.util.Map<String, Object> m, org.talend.concurrent.LinkedBlockingQueue<? extends Object> q, int i) {" + NL + "    \t\t\t\tsuper(m,i, \"";
  protected final String TEXT_18 = "\",q);" + NL + "    \t\t\t}" + NL + "    \t\t\tpublic void doWork() throws Exception {" + NL + "            \t\t";
  protected final String TEXT_19 = "Process(mapInstance);" + NL + "        \t\t}" + NL + "    \t\t}" + NL + "    \t\tjava.util.Map<String, Object> origGlobalMap = (java.util.Map<String,Object>)globalMap.get(\"BACKUP_GLOBAL_MAP_";
  protected final String TEXT_20 = "\");" + NL + "    \t\tInteger threadId = (Integer)globalMap.get(\"";
  protected final String TEXT_21 = "_THREAD_ID\");" + NL + "    \t\tTargetTask_";
  protected final String TEXT_22 = " task_";
  protected final String TEXT_23 = " = new TargetTask_";
  protected final String TEXT_24 = "(origGlobalMap,null,threadId);" + NL + "    \t\t" + NL + "    \t\tjava.util.concurrent.ExecutorService ex_";
  protected final String TEXT_25 = " = " + NL + "    \t\t\t(java.util.concurrent.ExecutorService)globalMap.get(\"EXECUTOR_SERVICE_";
  protected final String TEXT_26 = "\");" + NL + "    \t\tjava.util.concurrent.ExecutorCompletionService<Object> ecs_";
  protected final String TEXT_27 = " = " + NL + "    \t\t\t(java.util.concurrent.ExecutorCompletionService<Object>)globalMap.get(\"EXECUTOR_COMPLETION_SERVICE_";
  protected final String TEXT_28 = "\");" + NL + "    \t\tjava.util.List<java.util.concurrent.Future<Object>> futures_";
  protected final String TEXT_29 = " = " + NL + "    \t\t\t(java.util.List<java.util.concurrent.Future<Object>>)globalMap.get(\"FUTURES_LIST_";
  protected final String TEXT_30 = "\");" + NL + "    \t\tjava.util.List<org.talend.concurrent.ParallelTask> tasks_";
  protected final String TEXT_31 = " = " + NL + "    \t\t\t(java.util.List<org.talend.concurrent.ParallelTask>)globalMap.get(\"TASKS_";
  protected final String TEXT_32 = "\");" + NL + "    \t\t" + NL + "    \t\ttasks_";
  protected final String TEXT_33 = ".add(task_";
  protected final String TEXT_34 = ");" + NL + "    \t\tfutures_";
  protected final String TEXT_35 = ".add(ecs_";
  protected final String TEXT_36 = ".submit(task_";
  protected final String TEXT_37 = "));" + NL + "    \t\tglobalMap.put(\"STARTED_";
  protected final String TEXT_38 = "\",Boolean.TRUE);" + NL + "    \t\tjava.util.List<java.util.Map<String,Object>> mapsList_";
  protected final String TEXT_39 = " = " + NL + "    \t\t\t(java.util.List<java.util.Map<String,Object>>)globalMap.get(\"THREAD_MAPS_";
  protected final String TEXT_40 = "_\"+jobName);" + NL + "    \t\tif (mapsList_";
  protected final String TEXT_41 = " != null) {" + NL + "    \t\t\tfor (java.util.Map<String,Object> map : mapsList_";
  protected final String TEXT_42 = ") {" + NL + "    \t\t\t\tif (globalMap != map) {" + NL + "    \t\t\t\t\tmap.put(\"STARTED_";
  protected final String TEXT_43 = "\",Boolean.TRUE);" + NL + "    \t\t\t\t}" + NL + "    \t\t\t}" + NL + "    \t\t}" + NL + "    \t}" + NL + "    }" + NL + "    int counter_";
  protected final String TEXT_44 = " = 0;" + NL + "    org.talend.concurrent.LinkedBlockingQueue<";
  protected final String TEXT_45 = "Struct> outputQueue_";
  protected final String TEXT_46 = " = new org.talend.concurrent.LinkedBlockingQueue<";
  protected final String TEXT_47 = "Struct>(";
  protected final String TEXT_48 = ");" + NL + "    org.talend.concurrent.LinkedBlockingQueue<org.talend.concurrent.LinkedBlockingQueue<";
  protected final String TEXT_49 = "Struct>> queueQueue_";
  protected final String TEXT_50 = " = " + NL + "    \t(org.talend.concurrent.LinkedBlockingQueue<org.talend.concurrent.LinkedBlockingQueue<";
  protected final String TEXT_51 = "Struct>>)globalMap.get(\"OUTQUEUE_";
  protected final String TEXT_52 = "\");" + NL + "    if (queueQueue_";
  protected final String TEXT_53 = " == null) {" + NL + "    \tqueueQueue_";
  protected final String TEXT_54 = " = new org.talend.concurrent.LinkedBlockingQueue<org.talend.concurrent.LinkedBlockingQueue<";
  protected final String TEXT_55 = "Struct>>();" + NL + "    \tglobalMap.put(\"OUTQUEUE_";
  protected final String TEXT_56 = "\",queueQueue_";
  protected final String TEXT_57 = ");" + NL + "    }" + NL + "    queueQueue_";
  protected final String TEXT_58 = ".offer(outputQueue_";
  protected final String TEXT_59 = ");" + NL + "    " + NL + "    java.util.concurrent.atomic.AtomicInteger beginCnt_";
  protected final String TEXT_60 = " = (java.util.concurrent.atomic.AtomicInteger)globalMap.get(\"BEGIN_COUNT_";
  protected final String TEXT_61 = "\");" + NL + "    if (beginCnt_";
  protected final String TEXT_62 = " == null) {" + NL + "    \tbeginCnt_";
  protected final String TEXT_63 = " = new java.util.concurrent.atomic.AtomicInteger(1);" + NL + "    } else {" + NL + "    \tbeginCnt_";
  protected final String TEXT_64 = ".incrementAndGet();" + NL + "    }" + NL + "    globalMap.put(\"BEGIN_COUNT_";
  protected final String TEXT_65 = "\",beginCnt_";
  protected final String TEXT_66 = ");" + NL + "    globalMap.put(\"STARTING_";
  protected final String TEXT_67 = "\",Boolean.TRUE);" + NL + "    globalMap.put(\"counter_";
  protected final String TEXT_68 = "\",counter_";
  protected final String TEXT_69 = ");" + NL + "    globalMap.put(\"outputQueue_";
  protected final String TEXT_70 = "\",outputQueue_";
  protected final String TEXT_71 = ");";
  protected final String TEXT_72 = NL + "\t" + NL + "\t";
  protected final String TEXT_73 = NL + "\t\tlog.info(\"";
  protected final String TEXT_74 = "[\" + Thread.currentThread().getName() + \"] - Start to collect data partitioned by ";
  protected final String TEXT_75 = ".\");" + NL + "\t";
  protected final String TEXT_76 = NL + "try {" + NL + "\t//when we get to this point the only thing that will have run since we've set this" + NL + "\t//up are the lookups with zero rows, any new keys won't be overwritten. " + NL + "\t//we want to populate all of those keys with the values we've already cached." + NL + "\tjava.util.Map<String,Object> backupGlobalMap_";
  protected final String TEXT_77 = " = (java.util.Map<String,Object>)globalMap.get(\"BACKUP_GLOBAL_MAP_";
  protected final String TEXT_78 = "\");" + NL + "\tjava.util.Set<String> keysBackup_";
  protected final String TEXT_79 = " = backupGlobalMap_";
  protected final String TEXT_80 = ".keySet();" + NL + "\t    org.talend.concurrent.LinkedBlockingQueue<";
  protected final String TEXT_81 = "Struct> rowQueue_";
  protected final String TEXT_82 = " = (org.talend.concurrent.LinkedBlockingQueue<";
  protected final String TEXT_83 = "Struct>)globalMap.get(\"QUEUE_";
  protected final String TEXT_84 = "\");" + NL + "        " + NL + "        " + NL + "        boolean finished_";
  protected final String TEXT_85 = " = globalMap.get(\"FINISHED_";
  protected final String TEXT_86 = "\") == null ? true : (Boolean)globalMap.get(\"FINISHED_";
  protected final String TEXT_87 = "\");" + NL + "        while (rowQueue_";
  protected final String TEXT_88 = " == null && !finished_";
  protected final String TEXT_89 = ") {" + NL + "        \tThread.sleep(1);" + NL + "        \trowQueue_";
  protected final String TEXT_90 = " =(org.talend.concurrent.LinkedBlockingQueue<";
  protected final String TEXT_91 = "Struct>)globalMap.get(\"QUEUE_";
  protected final String TEXT_92 = "\");" + NL + "        }" + NL + "        int counter_";
  protected final String TEXT_93 = " = 0;" + NL + "\t\tint avail_";
  protected final String TEXT_94 = " = 0;" + NL + "int emptyCounter_";
  protected final String TEXT_95 = " = 0;" + NL + "\t\twhile (rowQueue_";
  protected final String TEXT_96 = " != null && (!finished_";
  protected final String TEXT_97 = " || (finished_";
  protected final String TEXT_98 = " && !rowQueue_";
  protected final String TEXT_99 = ".isEmpty()))) {" + NL + "\t\t\t  " + NL + "\t\t    if (!rowQueue_";
  protected final String TEXT_100 = ".isEmpty()) {" + NL + "\t\t    \temptyCounter_";
  protected final String TEXT_101 = " = 0;" + NL + "\t\t        " + NL + "\t\t    ";
  protected final String TEXT_102 = NL + "\t\t   \t\t";
  protected final String TEXT_103 = "Struct in_";
  protected final String TEXT_104 = " = rowQueue_";
  protected final String TEXT_105 = ".take();" + NL + "\t\t   \t\t";
  protected final String TEXT_106 = " = new ";
  protected final String TEXT_107 = "Struct();" + NL + "  \t\t\t\tcounter_";
  protected final String TEXT_108 = "++;" + NL + "    \t\t\t";
  protected final String TEXT_109 = NL + "    \t\t\t\t";
  protected final String TEXT_110 = ".";
  protected final String TEXT_111 = " = in_";
  protected final String TEXT_112 = ".";
  protected final String TEXT_113 = ";" + NL + "\t\t        ";
  protected final String TEXT_114 = NL + "    \t\t";
  protected final String TEXT_115 = NL + "    \t\tlog.debug(\"";
  protected final String TEXT_116 = "[\" + Thread.currentThread().getName() + \"] - Collecting the record : \" " + NL + "    \t\t";
  protected final String TEXT_117 = NL + "    \t\t";
  protected final String TEXT_118 = " + ";
  protected final String TEXT_119 = ".";
  protected final String TEXT_120 = NL + "\t\t    ";
  protected final String TEXT_121 = " + \"|\" + ";
  protected final String TEXT_122 = ".";
  protected final String TEXT_123 = NL + "\t\t    ";
  protected final String TEXT_124 = ");" + NL + "\t\t    ";
  protected final String TEXT_125 = NL + "\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();	
    List<IMetadataTable> metadatas = node.getMetadataList();
	IMetadataTable metadata = metadatas.get(0);
    List<? extends IConnection> connsout = node.getOutgoingConnections();
    String connName = null;
    String linkedPartitionerName = null;//ElementParameterParser.getValue(node,"__LINKED_PARTITIONER__");
    IProcess process = node.getProcess();
    INode linkedNode = null;
    String linkedConnName = null;
    
    List<? extends IConnection> startsConns = node.getIncomingConnections(EConnectionType.STARTS);
    for (int i = 0; i < startsConns.size(); i++) {
    	IConnection conn = startsConns.get(i);
    	INode source = conn.getSource();
    	if (source.getUniqueName().startsWith("tPartitioner") && linkedNode == null) {
    		linkedPartitionerName = source.getUniqueName();
    		linkedNode = source;
    		if (linkedNode.getIncomingConnections().size() == 1) {
    			IConnection iconn = linkedNode.getIncomingConnections().get(0);
    			linkedConnName = iconn.getName();
    		}
    	}
    }
    if (node.getIncomingConnections().size() == 1) {
       	IConnection conn = node.getIncomingConnections().get(0);
       	connName = conn.getName();
    }	
    List<IMetadataColumn> columnsout = metadata.getListColumns();
    String outName = null;
    if (connsout != null) {
       	for (int i = 0; i < connsout.size(); i++) {
           	IConnection connout = connsout.get(i);
           	
           	if (connout.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
           		outName = connout.getName();
           	}
        }
    }    
    boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
    
    //init depart part
	List<? extends INode> departitioners= node.getProcess().getNodesOfType("tDepartitioner");
	if (departitioners!=null && departitioners.size() > 0 ) {
		for (INode dnode : departitioners) {
			if(dnode == null) {
				continue;
			}
            INode startNode = org.talend.core.model.utils.NodeUtil.getSubProcessStartNode(dnode);
            if(node == startNode) {
            	INode departitionerNode = dnode;

    stringBuffer.append(TEXT_1);
    
	String departitionerNodeId = departitionerNode.getUniqueName();
    String targetRecollector = null;//have to have one for this to exist...
    List<? extends INode> listRecoll= departitionerNode.getProcess().getNodesOfType("tRecollector");
	if (listRecoll!=null && listRecoll.size() > 0 ) {
		for (INode rnode : listRecoll) {
			if (ElementParameterParser.getValue(rnode,"__DEPART_COMPONENT__").equals(departitionerNodeId)) {
				targetRecollector = rnode.getUniqueName();
				break;
			}
		}
	}
	String startCid = cid;
	String queueSize = ElementParameterParser.getValue(departitionerNode,"__QUEUE_SIZE__");
    IProcess departProcess = departitionerNode.getProcess();
    INode partLinkedNode = null;
    
    List<? extends INode> partitioners = departProcess.getNodesOfType("tPartitioner");
    for (int i = 0; i < partitioners.size(); i++) {
    	INode partitioner = partitioners.get(i);
    	if (partitioner.getUniqueName().equals(linkedPartitionerName)) {
    		partLinkedNode = partitioner;
    	}
    }
    boolean isReusing = "true".equalsIgnoreCase(ElementParameterParser.getValue(partLinkedNode,"__IS_REUSEE__"));
    String srcForPorts = ElementParameterParser.getValue(linkedNode,"__SRC_FOR_PORTS__");
    if (isReusing) {
		boolean found = false;
    	for (int i = 0; i < partitioners.size() && isReusing && !found; i++) {
        	INode tmp = partitioners.get(i);
        	if (tmp.getUniqueName().equals(srcForPorts)) {
        		linkedNode = tmp;
        		found=true;
        	}
    	}
	}
	
	String tPartitioner_cid = "";
	
	INode tCollector = node;
	List<? extends IConnection> inConns = tCollector.getIncomingConnections(EConnectionType.STARTS);
	if (inConns !=null && inConns.size() > 0 ) {
		tPartitioner_cid = inConns.get(0).getSource().getUniqueName();
	}
	
	String connNameToDepart = null;
    if (departitionerNode.getIncomingConnections().size() == 1) {    
        IConnection conn = departitionerNode.getIncomingConnections().get(0);
        connNameToDepart = conn.getName();
    }

    stringBuffer.append(TEXT_2);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_3);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(queueSize );
    stringBuffer.append(TEXT_6);
    stringBuffer.append(connNameToDepart );
    stringBuffer.append(TEXT_7);
    if (targetRecollector!=null) {
    stringBuffer.append(TEXT_8);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(targetRecollector);
    stringBuffer.append(TEXT_10);
    }
    stringBuffer.append(TEXT_11);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_12);
    }
    stringBuffer.append(TEXT_13);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(targetRecollector);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(targetRecollector);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(targetRecollector);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(tPartitioner_cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(startCid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(tPartitioner_cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(tPartitioner_cid);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(tPartitioner_cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(tPartitioner_cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(targetRecollector);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(tPartitioner_cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(targetRecollector);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(connNameToDepart);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(connNameToDepart);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(queueSize);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(connNameToDepart);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(connNameToDepart);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(connNameToDepart);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(departitionerNodeId);
    stringBuffer.append(TEXT_71);
    
            }
		}
	}

    stringBuffer.append(TEXT_72);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(linkedPartitionerName );
    stringBuffer.append(TEXT_75);
    }
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(linkedPartitionerName );
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(linkedConnName);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(linkedConnName);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(linkedConnName);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    
	if (outName != null) {
       
    stringBuffer.append(TEXT_102);
    stringBuffer.append(linkedConnName);
    stringBuffer.append(TEXT_103);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(outName);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(outName);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_108);
    
    			for (int i = 0; i < columnsout.size(); i++) { //have to assume identical schema
    				String label = columnsout.get(i).getLabel();
    				
    stringBuffer.append(TEXT_109);
    stringBuffer.append(outName);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_112);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_113);
    
    			}
    			
    stringBuffer.append(TEXT_114);
    
    		if(isLog4jEnabled){
    		
    stringBuffer.append(TEXT_115);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_116);
    
    			for (int i = 0; i < columnsout.size(); i++) {
    				String label = columnsout.get(i).getLabel();
    		
    stringBuffer.append(TEXT_117);
    
					if( i== 0 ) {
			
    stringBuffer.append(TEXT_118);
    stringBuffer.append(outName);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_120);
    
		           	} else {
		    
    stringBuffer.append(TEXT_121);
    stringBuffer.append(outName);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_123);
    
		    		}
		    	}
		    
    stringBuffer.append(TEXT_124);
    
		    }
		    
    
    }
        
    stringBuffer.append(TEXT_125);
    return stringBuffer.toString();
  }
}
