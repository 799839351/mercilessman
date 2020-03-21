package core.model;

public enum FileTypeEnum implements BaseEnum {
    EXCLE_XLS("excle_xls", ".xls"),
    EXCLE_XLSX("excle_xlsx", ".xlsx"),
    PDF("pdf", ".pdf"),
    WORLD("world", ".docx"),
    IMAGE_JPG("jpg", ".jpg"),
    IMAGE_PNG("png", ".png"),
    IMAGE_GIF("gif", ".gif"),
    IMAGE_BMP("bmp", ".bmp"),
    UNKNOW("unknown", "未知文件格式");

    private String code;
    private String desc;

    private FileTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static Boolean imageFile(FileTypeEnum typeEnum) {
        return IMAGE_JPG == typeEnum || IMAGE_PNG == typeEnum || IMAGE_GIF == typeEnum || IMAGE_BMP == typeEnum;
    }

    public static Boolean compressAble(FileTypeEnum typeEnum) {
        return IMAGE_JPG == typeEnum || IMAGE_PNG == typeEnum;
    }

    public static Boolean watermarkAble(FileTypeEnum typeEnum) {
        return IMAGE_JPG == typeEnum || IMAGE_PNG == typeEnum;
    }

    public static Boolean excelFile(FileTypeEnum typeEnum) {
        return EXCLE_XLS == typeEnum || EXCLE_XLSX == typeEnum;
    }
}

