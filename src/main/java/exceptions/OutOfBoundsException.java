package exceptions;

public class OutOfBoundsException extends RuntimeException {
    public OutOfBoundsException(String message){
        super (message);
    }
}