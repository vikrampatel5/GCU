package exceptions;

public class PointExistsException extends RuntimeException {
    public PointExistsException(String message){
        super(message);
    }
}