/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.demo.cases.case23.flinkx;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.net.ftp.FTP.*;

/**
 * The concrete Ftp Utility class used for standard ftp
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpHandler implements IFtpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FtpHandler.class);

    private static final String DISCONNECT_FAIL_MESSAGE = "Failed to disconnect from ftp server";

    private FTPClient ftpClient = null;

    private static final String SP = "/";

    public FTPClient getFtpClient() {
        return ftpClient;
    }

    @Override
    public void loginFtpServer(FtpConfig ftpConfig) {
        ftpClient = new FTPClient();
        try {
            // 连接
            ftpClient.connect(ftpConfig.getHost(), ftpConfig.getPort());
            // 登录
            ftpClient.login(ftpConfig.getUsername(), ftpConfig.getPassword());
            ftpClient.setFileType(BINARY_FILE_TYPE);
            // 不需要写死ftp server的OS TYPE,FTPClient getSystemType()方法会自动识别
            ftpClient.setConnectTimeout(ftpConfig.getTimeout());
            ftpClient.setDataTimeout(ftpConfig.getTimeout());
            if (EFtpMode.PASV.name().equals(ftpConfig.getConnectPattern())) {
                ftpClient.enterRemotePassiveMode();
                ftpClient.enterLocalPassiveMode();
            } else if (EFtpMode.PORT.name().equals(ftpConfig.getConnectPattern())) {
                ftpClient.enterLocalActiveMode();
            }
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                        "message:host =" + ftpConfig.getHost() + ",username = " + ftpConfig.getUsername() + ",port =" + ftpConfig.getPort());
                LOG.error(message);
                throw new RuntimeException(message);
            }
            //设置命令传输编码
            String fileEncoding = System.getProperty("file.encoding");
            ftpClient.setControlEncoding(fileEncoding);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int pwd(){
        int result = 0;
        try {
            ftpClient.pwd();
        } catch (IOException e) {
            String error = "Print current path throw exception";
            LOG.error(error, e);
            throw new RuntimeException(error, e);
        }
        return result;
    }

    @Override
    public boolean isConnected() {
        return ftpClient.isConnected();
    }

    @Override
    public void logoutFtpServer() {
        if (ftpClient.isConnected()) {
            try {
                ftpClient.logout();
            } catch (IOException e) {
                LOG.error(DISCONNECT_FAIL_MESSAGE);
                throw new RuntimeException(e);
            }finally {
                if(ftpClient.isConnected()){
                    try {
                        ftpClient.disconnect();
                    } catch (IOException e) {
                        LOG.error(DISCONNECT_FAIL_MESSAGE);
                        throw new RuntimeException(e);
                    }
                }

            }
        }
    }

    @Override
    public boolean isDirExist(String directoryPath) {
        String originDir = null;
        try {
            originDir = ftpClient.printWorkingDirectory();
            return ftpClient.changeWorkingDirectory(new String(directoryPath.getBytes(StandardCharsets.UTF_8), FTP.DEFAULT_CONTROL_ENCODING));
        } catch (IOException e) {
            String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
            LOG.error(message);
            throw new RuntimeException(message, e);
        } finally {
            if(originDir != null) {
                try {
                    ftpClient.changeWorkingDirectory(originDir);
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    @Override
    public boolean isFileExist(String filePath) {
        boolean isExitFlag = false;
        try {
            FTPFile[] ftpFiles = ftpClient.listFiles(new String(filePath.getBytes(StandardCharsets.UTF_8),FTP.DEFAULT_CONTROL_ENCODING));
            if (ftpFiles.length == 1 && ftpFiles[0].isFile()) {
                isExitFlag = true;
            }
        } catch (IOException e) {
            String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
            LOG.error(message, e);
            throw new RuntimeException(e);
        }
        return isExitFlag;
    }


    @Override
    public List<String> getFiles(String path) {
        List<String> sources = new ArrayList<>();
        if(isDirExist(path)) {
            if(!path.endsWith(SP)) {
                path = path + SP;
            }
            try {
                FTPFile[] ftpFiles = ftpClient.listFiles(new String(path.getBytes(StandardCharsets.UTF_8),FTP.DEFAULT_CONTROL_ENCODING));
                if(ftpFiles != null) {
                    for(FTPFile ftpFile : ftpFiles) {
                        sources.addAll(getFiles(path + ftpFile.getName()));
                    }
                }
            } catch (IOException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }

        } else if(isFileExist(path)) {
            sources.add(path);
            return sources;
        }

        return sources;
    }

    @Override
    public void mkDirRecursive(String directoryPath){
        StringBuilder dirPath = new StringBuilder();
        dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
        String[] dirSplit = StringUtils.split(directoryPath,IOUtils.DIR_SEPARATOR_UNIX);
        String message = String.format("创建目录:%s时发生异常,请确认与ftp服务器的连接正常,拥有目录创建权限", directoryPath);
        try {
            // ftp server不支持递归创建目录,只能一级一级创建
            for(String dirName : dirSplit){
                dirPath.append(dirName);
                boolean mkdirSuccess = mkDirSingleHierarchy(dirPath.toString());
                dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                if(!mkdirSuccess){
                    throw new RuntimeException(message);
                }
            }
        } catch (IOException e) {
            message = String.format("%s, errorMessage:%s", message,
                    e.getMessage());
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }


    private boolean mkDirSingleHierarchy(String directoryPath) throws IOException {
        boolean isDirExist = this.ftpClient
                .changeWorkingDirectory(directoryPath);
        // 如果directoryPath目录不存在,则创建
        if (!isDirExist) {
            int replayCode = this.ftpClient.mkd(directoryPath);
            if (replayCode != FTPReply.COMMAND_OK
                    && replayCode != FTPReply.PATHNAME_CREATED) {
                return false;
            }
        }
        return true;
    }

    @Override
    public OutputStream getOutputStream(String filePath) {
        try {
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0,
//                    StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
                    StringUtils.lastIndexOf(filePath, "/"));
            this.ftpClient.changeWorkingDirectory(parentDir);
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.ftpClient
                    .appendFileStream(filePath);
            String message = String.format(
                    "打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath,
                    filePath);
            if (null == writeOutputStream) {
                throw new RuntimeException(message);
            }

            return writeOutputStream;
        } catch (IOException e) {
            String message = String.format(
                    "写出文件 : [%s] 时出错,请确认文件:[%s]存在且配置的用户有权限写, errorMessage:%s",
                    filePath, filePath, e.getMessage());
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    private void printWorkingDirectory() {
        try {
            LOG.info(String.format("current working directory:%s",
                    this.ftpClient.printWorkingDirectory()));
        } catch (Exception e) {
            LOG.warn(String.format("printWorkingDirectory error:%s",
                    e.getMessage()));
        }
    }

    @Override
    public void deleteAllFilesInDir(String dir, List<String> exclude) {
        if(isDirExist(dir)) {
            if(!dir.endsWith(SP)) {
                dir = dir + SP;
            }

            try {
                FTPFile[] ftpFiles = ftpClient.listFiles(new String(dir.getBytes(StandardCharsets.UTF_8),FTP.DEFAULT_CONTROL_ENCODING));
                if(ftpFiles != null) {
                    for(FTPFile ftpFile : ftpFiles) {
                        if(CollectionUtils.isNotEmpty(exclude) && exclude.contains(ftpFile.getName())){
                            continue;
                        }
                        deleteAllFilesInDir(dir + ftpFile.getName(), exclude);
                    }
                }

                if(CollectionUtils.isEmpty(exclude)){
                    ftpClient.rmd(dir);
                }
            } catch (IOException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        } else if(isFileExist(dir)) {
            try {
                ftpClient.deleteFile(dir);
            } catch (IOException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public InputStream getInputStream(String filePath) {
        try {
            InputStream is = ftpClient.retrieveFileStream(new String(filePath.getBytes(StandardCharsets.UTF_8),FTP.DEFAULT_CONTROL_ENCODING));
            return is;
        } catch (IOException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public List<String> listDirs(String path) {
        List<String> sources = new ArrayList<>();
        if(isDirExist(path)) {
            if(!path.endsWith(SP)) {
                path = path + SP;
            }

            try {
                FTPFile[] ftpFiles = ftpClient.listFiles(new String(path.getBytes(StandardCharsets.UTF_8),FTP.DEFAULT_CONTROL_ENCODING));
                if(ftpFiles != null) {
                    for(FTPFile ftpFile : ftpFiles) {
                        sources.add(path + ftpFile.getName());
                    }
                }
            } catch (IOException e) {
                LOG.error("", e);
                throw new RuntimeException(e);
            }
        }

        return sources;
    }

    @Override
    public void rename(String oldPath, String newPath) {
        try {
            ftpClient.rename(oldPath, newPath);
        } catch (IOException e) {
            String message = String.format("重命名文件：[%s] 到 [%s] 时发生I/O异常,请确认与ftp服务器的连接正常", oldPath, newPath);
            LOG.error(message, e);
            throw new RuntimeException(e);
        }
    }

    //设置处理多个文件
    public boolean completePendingCommand(){
        try {
            return ftpClient.completePendingCommand();
        } catch (IOException e) {
            String message = String.format("通知");
            LOG.error(message, e);
            throw new RuntimeException(e);
        }
    }
}