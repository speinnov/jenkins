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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * created by rdubois on 19 juin 2015 Detailled comment
 *
 */
public class BigDataUtil {

    private static org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(BigDataUtil.class);

    private static final String HADOOP_HOME_DIR = "/winutils"; //$NON-NLS-1$

    private static final String WINUTILS_EXE = "winutils.exe"; //$NON-NLS-1$

    private static final String HADOOP_HOME_DIR_SYSPROP = "hadoop.home.dir"; //$NON-NLS-1$

    private static final String HADOOP_HOME_DIR_ENV = "HADOOP_HOME"; //$NON-NLS-1$

    private static String OS = System.getProperty("os.name").toLowerCase(); //$NON-NLS-1$

    private static boolean installHadoopHomeDirectory(java.io.File binFolder) {
        boolean folderCreated = true;
        if (!binFolder.exists()) {
            folderCreated = binFolder.mkdirs();
        }
        return folderCreated;
    }

    private static boolean environmentVariablesContainHadoopHomeDir() {
        boolean envExists = false;
        Map<String, String> env = System.getenv();
        for (String envName : env.keySet()) {
            if (HADOOP_HOME_DIR_ENV.equals(envName)) {
                envExists = true;
                break;
            }
        }
        return envExists;
    }

    private static boolean systemPropertiesContainHadoopHomeDir() {
        boolean sysPropExists = false;
        if (System.getProperties().containsKey(HADOOP_HOME_DIR_SYSPROP)) {
            sysPropExists = true;
        }
        return sysPropExists;
    }

    public static boolean installWinutils(String scratchdir, String winutilsFilePath) throws Exception {
        boolean isWindowsOS = OS.indexOf("win") >= 0; //$NON-NLS-1$
        if (isWindowsOS) {
            boolean environmentVariablesContainHadoopHomeDir = environmentVariablesContainHadoopHomeDir();
            boolean systemPropertiesContainHadoopHomeDir = systemPropertiesContainHadoopHomeDir();
            if (!environmentVariablesContainHadoopHomeDir && !systemPropertiesContainHadoopHomeDir) {
                LOG.debug(HADOOP_HOME_DIR_ENV + " not found in the environment variables."); //$NON-NLS-1$
                LOG.debug(HADOOP_HOME_DIR_SYSPROP + " not found in the system properties."); //$NON-NLS-1$
                java.io.File binFolder = new java.io.File(scratchdir + HADOOP_HOME_DIR + "/bin"); //$NON-NLS-1$
                boolean hadoopHomeDirectoryExists = installHadoopHomeDirectory(binFolder);
                if (hadoopHomeDirectoryExists) {
                    File winutil = new File(binFolder.getAbsolutePath() + "/" + WINUTILS_EXE); //$NON-NLS-1$
                    if (winutil != null && !winutil.exists()) {
                        java.nio.file.Files.copy(java.nio.file.FileSystems.getDefault().getPath(winutilsFilePath),
                                java.nio.file.FileSystems.getDefault().getPath(binFolder.getAbsolutePath() + "/" + WINUTILS_EXE)); //$NON-NLS-1$
                    }
                    System.setProperty(HADOOP_HOME_DIR_SYSPROP, new File(scratchdir + "/winutils").getAbsolutePath()); //$NON-NLS-1$
                    LOG.debug(HADOOP_HOME_DIR_SYSPROP + " = " + System.getProperty(HADOOP_HOME_DIR_SYSPROP)); //$NON-NLS-1$
                } else {
                    throw new Exception("Unable to install the hadoop home directory. Please do it by yourself."); //$NON-NLS-1$
                }
            } else if (!systemPropertiesContainHadoopHomeDir && environmentVariablesContainHadoopHomeDir) {
                LOG.debug(HADOOP_HOME_DIR_ENV + " found in the environment variables."); //$NON-NLS-1$
                LOG.debug(HADOOP_HOME_DIR_SYSPROP + " not found in the environment variables."); //$NON-NLS-1$
                String hadoopHomeDir = System.getenv(HADOOP_HOME_DIR_ENV);
                if (!new java.io.File(hadoopHomeDir + "/bin/" + WINUTILS_EXE).exists()) { //$NON-NLS-1$
                    throw new FileNotFoundException(
                            "The hadoop home directory (" + HADOOP_HOME_DIR_ENV + ") doesn't contain the required " //$NON-NLS-1$ //$NON-NLS-2$
                                    + WINUTILS_EXE + " binary."); //$NON-NLS-1$
                }
            } else {
                LOG.debug(HADOOP_HOME_DIR_ENV + " found in the environment variables."); //$NON-NLS-1$
                LOG.debug(HADOOP_HOME_DIR_SYSPROP + " found in the environment variables."); //$NON-NLS-1$s
                String hadoopHomeDir = System.getProperty(HADOOP_HOME_DIR_SYSPROP);
                if (!new java.io.File(hadoopHomeDir + "/bin/" + WINUTILS_EXE) //$NON-NLS-1$
                        .exists()) {
                    throw new FileNotFoundException(
                            "The hadoop home directory (" + HADOOP_HOME_DIR_SYSPROP + ") doesn't contain the required " //$NON-NLS-1$ //$NON-NLS-2$
                                    + WINUTILS_EXE + " binary."); //$NON-NLS-1$
                }
            }

        }
        return true;
    }
}
