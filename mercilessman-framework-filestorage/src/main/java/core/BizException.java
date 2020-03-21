package core;

public class BizException extends Throwable {
    private static final long serialVersionUID = 1243081562453061652L;
    private String errCode;
    private String byproduct;

    public BizException(String errCode, String message) {
        super(message, (Throwable)null, false, false);
        this.errCode = errCode;
    }

    public BizException(String message) {
        super(message, (Throwable)null, false, true);
        this.errCode = "D9999";
    }

    public BizException(Throwable cause) {
        super(cause);
        this.errCode = "J9999";
    }

    public String getErrCode() {
        return this.errCode;
    }

    public String getByproduct() {
        return this.byproduct;
    }

    public final BizException setByproduct(String byproduct) {
        this.byproduct = byproduct;
        return this;
    }
}
