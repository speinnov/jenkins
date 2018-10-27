// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package routines.system;

import java.util.ArrayList;

/**
 * DOC xzhang class global comment. Detailled comment
 */
public class ParallelThreadPool {

    private volatile boolean stopAllWorkers = false;

    private ParallelThread errorThread = null;

    private TalendThreadResult threadResult = null;

    private ParallelThread[] threads;

    private int poolMaxSize = 0;

    private int poolCurrentSize = 0;

    /**
     * init the thread list
     * 
     * @param poolSize the size of pool
     */
    public ParallelThreadPool(int poolSize) {
        this.threadResult = new TalendThreadResult();
        this.poolMaxSize = Math.max(1, poolSize);
        this.threads = new ParallelThread[this.poolMaxSize];
    }

    /**
     * add and run the thread
     * 
     * @param pt
     */
    public void execThread(ParallelThread pt) throws Exception {
        checkException();

        if (!stopAllWorkers) {
            pt.setThreadID(poolCurrentSize);
            this.threads[poolCurrentSize++] = pt;
            pt.start();
        }
    }

    public boolean isFull() {
        return this.poolCurrentSize >= this.poolMaxSize;
    }

    /**
     * get free thread for setting new rows
     * 
     * @return
     */

    public ParallelThread getFreeThread() throws Exception {
        checkException();

        while (!stopAllWorkers) {
            for (ParallelThread tmp : this.threads) {
                if (tmp != null && tmp.isFree()) {
                    return tmp;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
                // ExceptionHandler.process(e);
            }
        }
        return null;
    }

    /**
     * 
     * 
     * DOC stan_zhang Comment method "waitForEnd".
     */
    public void waitForEnd() throws Exception {
        try {
            if (!stopAllWorkers) {
                for (ParallelThread tmp : this.threads) {
                    // if there's little rows, threads isn't full, tmp will be null.
                    if (tmp != null) {
                        // make sure the parallel thread is waiting for buffer
                        tmp.waitForFree();
                        // set finish
                        tmp.finish();
                        // insert a empty buffer to break the waiting buffer
                        tmp.putBuffer(new ArrayList<String[]>());
                    }

                    checkException();
                }
            } else {
                checkException();
            }
            while (!stopAllWorkers) {
                boolean hasThreadWork = false;
                for (ParallelThread tmp : this.threads) {
                    if (tmp != null && tmp.isAlive()) {
                        hasThreadWork = true;
                    }
                }
                if (hasThreadWork) {
                    Thread.sleep(100);
                } else {
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            // ExceptionHandler.process(e);
        }
    }

    /**
     * DOC liuwu Comment method "setGlobalVariables". In order to fix the bug:TDI-28741
     * 
     * @param globalMap
     */
    public void setGlobalVariables(final java.util.Map globalMap) {
        for (ParallelThread tmp : this.threads) {
            if (tmp != null) {
                for (String key : tmp.globalMap.myMap.keySet()) {
                    if (key.contains("_NB_LINE")&& !globalMap.containsKey(key)) {
                        if (tmp.globalMap.get(key) instanceof Number) {
                            int tempValue = ((Number) tmp.globalMap.get(key)).intValue();
                            if (globalMap.get(key) == null) {
                                globalMap.put(key, tempValue);
                            } else {
                                globalMap.put(key, ((Number) tmp.globalMap.get(key)).intValue() + tempValue);
                            }
                        } else {
                            globalMap.put(key, tmp.globalMap.get(key));
                        }
                    }
                }
            }
        }
    }

    public synchronized void stopAllThreads() {
        if (!stopAllWorkers) {
            try {
                stopAllWorkers = true;
                for (ParallelThread tmp : this.threads) {
                    if (tmp != null) {
                        tmp.interrupt();
                        tmp.clearBuffer();
                        tmp.setFree(true);
                        tmp.finish();
                        while (tmp.isAlive()) {
                            Thread.sleep(100);
                        }
                    }
                }
            } catch (InterruptedException x) {
            }

        }
    }

    /**
     * get ErrorThread.
     * 
     * @return
     */
    public ParallelThread getErrorThread() {
        return errorThread;
    }

    /**
     * only keep the first ErrorThread
     * 
     * @param errorThread
     */
    public synchronized void setErrorThread(ParallelThread errorThread, Exception exception) {
        if (this.errorThread == null) {
            this.errorThread = errorThread;
        }
        if (getTalendThreadResult().getException() == null) {
            this.getTalendThreadResult().setException(exception);
        }
    }

    public TalendThreadResult getTalendThreadResult() {
        return threadResult;
    }

    private void checkException() throws Exception {
        if (this.getErrorThread() != null) {
            throw this.getTalendThreadResult().getException();
        }
    }
}
