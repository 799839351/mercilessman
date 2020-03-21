package core;

import core.model.FileExportRequest;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletResponse;

@Component
public interface FileService {

    void exportFile(HttpServletResponse paramHttpServletResponse,
                    FileExportRequest paramFileExportRequest) throws BizException;


}