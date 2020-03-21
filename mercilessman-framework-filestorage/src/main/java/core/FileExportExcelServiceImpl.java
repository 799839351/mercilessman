package core;

import core.model.ExcelExportRequest;
import core.model.FileExportRequest;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import java.io.OutputStream;

public class FileExportExcelServiceImpl implements FileExportService {
    private static final Logger log = LoggerFactory.getLogger(FileExportExcelServiceImpl.class);
    private static final PoiService poiService = new PoiServiceImpl();


    @Override
    public void exportFile(HttpServletResponse response, FileExportRequest request) throws Exception {
        ExcelExportRequest excelRequest = (ExcelExportRequest)request;
        excelRequest.check();
        response.setContentType("application/msexcel");
        OutputStream os = response.getOutputStream();
        Workbook workbook = poiService.getWorkbook(excelRequest.getDataList(), excelRequest.getClazz(), (String)null, request.getFileType());
        workbook.write(os);
        os.flush();
    }
}
