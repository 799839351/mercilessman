package core;

import core.model.FileTypeEnum;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.util.List;

public interface PoiService {
    <T> Workbook getWorkbook(List<T> var1, Class var2, String var3, FileTypeEnum var4) throws Exception;

    Sheet getSheet(Workbook var1, List<String> var2, String var3, String var4);

    <T> void write2Sheet(Sheet var1, List<T> var2, Integer var3) throws Exception;

    List<String> getSheetHeaders(Class var1);
}
