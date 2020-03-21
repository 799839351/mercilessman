package core;


import core.anno.ExcelColumnAnno;
import core.model.FileTypeEnum;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;

public class PoiServiceImpl implements PoiService {
    private Integer rowNumber = 0;
    private static final Integer EXCEL_MAX_EXPORT_NUM = 60000;

    public PoiServiceImpl() {
    }

    @Override
    public <T> Workbook getWorkbook(List<T> list, Class clazz, String title, FileTypeEnum fileType) throws Exception {
        int divide = list.size() / EXCEL_MAX_EXPORT_NUM;
        int mode = list.size() % EXCEL_MAX_EXPORT_NUM;
        int page = mode != 0 ? divide + 1 : divide;
        page = Math.max(1, page);
        Workbook workbook = FileTypeEnum.EXCLE_XLS == fileType ? new HSSFWorkbook() : new SXSSFWorkbook();
        List<String> headers = this.getSheetHeaders(clazz);

        for(int i = 1; i <= page; ++i) {
            String sheetName = "第" + i + "页";
            Sheet sheet = this.getSheet((Workbook)workbook, headers, sheetName, title);
            int toIndex = Math.min(list.size(), Math.min(i * EXCEL_MAX_EXPORT_NUM, list.size()));
            List<T> sheetData = list.subList((i - 1) * EXCEL_MAX_EXPORT_NUM, toIndex);
            this.write2Sheet(sheet, sheetData, headers.size());
        }

        return (Workbook)workbook;
    }

    @Override
    public Sheet getSheet(Workbook workbook, List<String> headers, String sheetName, String title) {
        Sheet sheet = workbook.createSheet(sheetName);
        this.setHSSFSheetHeaderStyle(sheet, headers.size(), title);
        Row headerRow;
        CellStyle headerCellStyle;
        if (StringUtils.isNotBlank(title)) {
            headerRow = sheet.createRow(this.rowNumber);
            Cell titleCell = headerRow.createCell(this.rowNumber);
            headerCellStyle = this.getCellStyle(workbook, (short)16, true);
            titleCell.setCellStyle(headerCellStyle);
            titleCell.setCellValue(title);
            this.rowNumber = this.rowNumber + 1;
        }

        headerRow = sheet.createRow(this.rowNumber);

        for(int loop = 0; loop < headers.size(); ++loop) {
            headerCellStyle = this.getCellStyle(workbook, (short)14, false);
            Cell headerCell = headerRow.createCell(loop);
            headerCell.setCellStyle(headerCellStyle);
            headerCell.setCellValue((String)headers.get(loop));
        }

        return sheet;
    }

    @Override
    public <T> void write2Sheet(Sheet sheet, List<T> list, Integer columnSize) throws Exception {
        if (!CollectionUtils.isEmpty(list)) {
            Iterator var4 = list.iterator();

            while(var4.hasNext()) {
                T instance = (T) var4.next();
                Row dataRow = sheet.createRow(this.rowNumber = this.rowNumber + 1);
                Class<?> clazz = instance.getClass();
                Field[] fields = clazz.getDeclaredFields();
                List<Cell> cells = this.getHSSFCell(dataRow, columnSize);
                Field[] var10 = fields;
                int var11 = fields.length;

                for(int var12 = 0; var12 < var11; ++var12) {
                    Field field = var10[var12];
                    if (field.isAnnotationPresent(ExcelColumnAnno.class)) {
                        ExcelColumnAnno annotation = (ExcelColumnAnno)field.getAnnotation(ExcelColumnAnno.class);
                        int index = annotation.index();
                        int size = annotation.size();
                        String name = field.getName();
                        name = name.substring(0, 1).toUpperCase() + name.substring(1);
                        Method method = clazz.getMethod("get" + name);
                        Object invoke = method.invoke(instance);
                        if (null != invoke) {
                            if (field.getType() == Date.class) {
                                String value = (new SimpleDateFormat(annotation.format())).format(invoke);
                                ((Cell)cells.get(index)).setCellValue(value);
                            } else if (size <= 0 || field.getType() != Collection.class && field.getType() != Set.class && field.getType() != List.class) {
                                ((Cell)cells.get(index)).setCellValue(String.valueOf(invoke));
                            } else {
                                Object[] collection = ((Collection)invoke).toArray();

                                for(int i = 0; i < collection.length; ++i) {
                                    ((Cell)cells.get(index + i)).setCellValue(String.valueOf(collection[i]));
                                }
                            }
                        }
                    }
                }
            }
        }

        this.rowNumber = 0;
    }

    @Override
    public List<String> getSheetHeaders(Class clazz) {
        if (clazz == null) {
            return null;
        } else {
            Field[] fields = clazz.getDeclaredFields();
            List<String> headers = Lists.newArrayList();
            Field[] var4 = fields;
            int var5 = fields.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                Field field = var4[var6];
                if (field.isAnnotationPresent(ExcelColumnAnno.class)) {
                    ExcelColumnAnno annotation = (ExcelColumnAnno)field.getAnnotation(ExcelColumnAnno.class);
                    int index = annotation.index();
                    String name = annotation.label();
                    int size = annotation.size();
                    if (size <= 0 || field.getType() != Collection.class && field.getType() != Set.class && field.getType() != List.class) {
                        headers.add(index, name);
                    } else {
                        for(int i = 0; i < size; ++i) {
                            headers.add(index + i, name + (i + 1));
                        }
                    }
                }
            }

            return headers;
        }
    }

    private List<Cell> getHSSFCell(Row row, Integer length) {
        List<Cell> cells = Lists.newArrayList();

        for(int loop = 0; loop < length; ++loop) {
            Cell cell = row.createCell(loop);
            cells.add(cell);
        }

        return cells;
    }

    public <T> List<String> getExcelColumns(Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        List<String> columns = Lists.newArrayList();
        Field[] var4 = fields;
        int var5 = fields.length;

        for(int var6 = 0; var6 < var5; ++var6) {
            Field field = var4[var6];
            if (field.isAnnotationPresent(ExcelColumnAnno.class)) {
                ExcelColumnAnno annotation = (ExcelColumnAnno)field.getAnnotation(ExcelColumnAnno.class);
                int index = annotation.index();
                String name = annotation.label();
                columns.add(index, name);
            }
        }

        return columns;
    }

    private void setHSSFSheetHeaderStyle(Sheet sheet, Integer headerSize, String title) {
        for(int loop = 0; loop < headerSize; ++loop) {
            sheet.setColumnWidth(loop, 7680);
        }

        int rowSplit = 1;
        int topRow = 1;
        if (StringUtils.isNotBlank(title)) {
            CellRangeAddress cra = new CellRangeAddress(0, 0, 0, headerSize - 1);
            sheet.addMergedRegion(cra);
            rowSplit = 2;
            topRow = 2;
        }

        sheet.createFreezePane(0, rowSplit, 0, topRow);
    }

    private CellStyle getCellStyle(Workbook workbook, short fontSize, Boolean isBold) {
        Font titleFont = workbook.createFont();
        titleFont.setBold(isBold);
        titleFont.setFontHeightInPoints(fontSize);
        CellStyle titleStyle = workbook.createCellStyle();
        titleStyle.setFont(titleFont);
        titleStyle.setAlignment(HorizontalAlignment.CENTER);
        return titleStyle;
    }
}
