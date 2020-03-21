package core;

import com.alibaba.fastjson.JSON;
import core.model.FileExportRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletResponse;

@Component

public class FileServiceImpl implements FileService {


    private final static Logger LOGGER = LoggerFactory.getLogger(FileServiceImpl.class);

    @Override
    public void exportFile(HttpServletResponse response, FileExportRequest request) throws BizException {
        LOGGER.info("[BUSINESS-EXPORT]: export file request-{}", JSON.toJSONString(request));
        try {
            FileExportService fileExportService = FileExportService.getFileExportService(request.getFileType());
            fileExportService.formatFileName(response, request);
            fileExportService.exportFile(response, request);
        } catch (Exception var4) {
            LOGGER.error("导出文件异常e-{}", var4);
            throw new BizException("导出文件异常");
        }
    }


}

