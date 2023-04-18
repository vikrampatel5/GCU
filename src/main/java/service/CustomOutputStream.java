package service;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class CustomOutputStream extends ObjectOutputStream {

    public CustomOutputStream(OutputStream out) throws IOException {
        super(out);
    }

    protected CustomOutputStream() throws IOException, SecurityException {
    }

    // Method of this class
    public void writeStreamHeader() throws IOException
    {
        return;
    }
}
