package simulation.generator.app;

import java.io.IOException;
import java.io.Writer;

import com.sun.istack.internal.NotNull;
import org.griphyn.vdl.dax.Filename;
import org.griphyn.vdl.classes.LFN;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Shishir Bharathi
 */
public class AppFilename extends Filename {

    private final Map<String, String> annotations;

    public AppFilename(String filename, int type, long size) {
        this(filename, type, size, LFN.XFER_MANDATORY, true);
        assert filename != null : "passed null as filename to AppFilename constructor, type = [" + type + "], size = [" + size + "]";
    }

    public AppFilename(String filename, int type, long size, int transfer, boolean register) {
        super(filename, type);
        this.annotations = new HashMap<String, String>();
        this.annotations.put("size", String.valueOf(size));
        super.setTransfer(transfer);
        super.setRegister(register);
        assert filename != null : "passed null as filename to AppFilename constructor, type = [" + type + "], size = [" + size + "], transfer = [" + transfer + "], register = [" + register + "]";
    }

    public Map<String, String> getAnnotations() {
        return this.annotations;
    }

    private String annotatedXML(String temp) {
        int idx = temp.indexOf("/>");
        StringBuilder result = new StringBuilder(temp.length() + 32);
        result.append(temp.substring(0, idx));
        for (Map.Entry<String, String> entry : this.annotations.entrySet()) {
            result.append(" " + entry.getKey() + "=\"" + entry.getValue() + "\"");
        }
        result.append("/>\n");

        return result.toString();
    }

    /** This is used to override file sizes to conform to a random memory model {@link simulation.generator.util.LinearModel} that has a dependency on file size. */
    public void setSize(long filesize) {
        this.annotations.put("size", Long.toString(filesize));
    }

    public long getSize() {
        String sizeStr = this.annotations.get("size");
        return Long.parseLong(sizeStr);
    }

    @Override
    public String shortXML(String indent, String namespace, int flag) {
        String temp = super.shortXML(indent, namespace, flag);

        return annotatedXML(temp);
    }

    @Override
    public void shortXML(Writer stream, String indent, String namespace, int flag) throws IOException {
        stream.write(shortXML(indent, namespace, flag));
    }

    @Override
    public String toXML(String indent, String namespace) {
        String temp = super.toXML(indent, namespace);
        temp = temp.replaceFirst("<filename", "<uses");

        return annotatedXML(temp);
    }

    @Override
    public void toXML(Writer stream, String indent, String namespace)
            throws IOException {
        stream.write(toXML(indent, namespace));
    }

    @Override
    public Object clone() {
        AppFilename f = (AppFilename) super.clone();
        f.annotations.putAll(this.annotations);

        return f;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AppFilename that = (AppFilename) o;

        return annotations != null ? annotations.equals(that.annotations) : that.annotations == null;
    }

    @Override
    public int hashCode() {
        return annotations != null ? annotations.hashCode() : 0;
    }
}