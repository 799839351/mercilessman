package core.model;

import core.model.FileExportRequest;
import core.model.FileTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExcelExportRequest<T> extends FileExportRequest {
    private List<T> dataList = new ArrayList();
    private Class clazz;

    public void exportParam(List<T> dataList, Class clazz, FileTypeEnum fileType, String fileName) {
        this.setDataList(dataList);
        this.setFileType(fileType);
        this.setClazz(clazz);
        this.setFileName(fileName);
    }

    public void check() {
        Objects.requireNonNull(this.clazz, "clazz 不能为空");
        Objects.requireNonNull(this.dataList, "导出数据不能为空");
        if (null == this.getFileType() || !FileTypeEnum.excelFile(this.getFileType())) {
            throw new IllegalArgumentException("不支持导出的文件类型");
        }
    }





}

