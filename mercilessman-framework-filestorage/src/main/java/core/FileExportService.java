package core;

import core.model.FileExportRequest;
import core.model.FileTypeEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import javax.servlet.http.HttpServletResponse;
import java.net.URLEncoder;
import java.util.Date;

public interface FileExportService {
    void exportFile(HttpServletResponse var1, FileExportRequest var2) throws Exception;

    static FileExportService getFileExportService(FileTypeEnum fileType) {
        switch(fileType) {
            case EXCLE_XLS:
            case EXCLE_XLSX:
                return new FileExportExcelServiceImpl();
            /*case PDF:
                return new FileExportPDFServiceImpl();
            case WORLD:
                return new FileExportWorldServiceImpl();*/
            default:
                throw new IllegalArgumentException("不支持的文件导出");
        }
    }

    default void formatFileName(HttpServletResponse response, FileExportRequest request) throws Exception {
        String fileName = (StringUtils.isNotBlank(request.getFileName()) ? request.getFileName() : DateFormatUtils.format(new Date(), "yyyyMMddHHmmssS")) + request.getFileType().getDesc();
        String fileNameURL = URLEncoder.encode(fileName, "UTF-8");
        response.setHeader("Content-Disposition", "attachment;filename=" + fileNameURL + ";filename*=utf-8''" + fileNameURL);
    }
}
