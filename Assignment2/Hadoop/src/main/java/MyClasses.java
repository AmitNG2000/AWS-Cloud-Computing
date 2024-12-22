import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class MyClasses {

    public class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);  // Specify the type of elements
        }
    }
}
