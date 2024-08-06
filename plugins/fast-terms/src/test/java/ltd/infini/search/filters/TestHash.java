package ltd.infini.search.filters;

import com.sun.jna.StringArray;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import net.openhft.hashing.LongHashFunction;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;



public class TestHash {
    public static void main(String[] args) throws IOException, URISyntaxException {

        //generateHash();

        String path = "/Users/medcl/Documents/java/filter-terms/src/test/resources/random-mini.txt";

        String str = new String(Files.readAllBytes(Paths.get(path)));
        String[] ids = str.split(",");

        RoaringBitmap rBitmap = new RoaringBitmap();
        for (int i = 0; i < ids.length; i++) {
//            Long l = Long.parseLong(ids[i]);
            Integer l = Integer.parseInt(ids[i]);
            System.out.println(l);
            System.out.println((int)l.intValue());
            rBitmap.add(l.intValue());
        }

        Integer byteSize=rBitmap.serializedSizeInBytes();
        ByteBuffer buffer =ByteBuffer.allocate(byteSize.intValue());
        rBitmap.serialize(buffer);

//        System.out.println(rBitmap.toString());
//        RoaringBitmap rBitmap2 = new RoaringBitmap();
//        rBitmap2.add(331227);
//        rBitmap2.and(rBitmap);
//        System.out.println(rBitmap2.toString());


        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(byteOut);
        rBitmap.serialize(out);

        byte[] dataArray = byteOut.toByteArray();


        String text = Base64.getEncoder().encodeToString(dataArray);
        System.out.println(str.toString().length());
        System.out.println(text.toString().length());
        System.out.println(text.toString());

    }

    private static void generateHash() {
        String text1="A";
//        final BytesRef bytes = new BytesRef(text1.toString());

        XXHashFactory factory = XXHashFactory.fastestInstance();
        byte[] data = new byte[0];
        try {
            data = text1.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        ByteArrayInputStream in = new ByteArrayInputStream(data);

        int seed = 0x9747b28c; // used to initialize the hash value, use whatever
        // value you want, but always the same
        StreamingXXHash32 hash32 = factory.newStreamingHash32(seed);
        byte[] buf = new byte[8]; // for real-world usage, use a larger buffer, like 8192 bytes
        for (;;) {
            int read = 0;
            try {
                read = in.read(buf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (read == -1) {
                break;
            }
            hash32.update(buf, 0, read);
        }
        Integer hash = hash32.getValue();
        System.out.println(hash);

//        final long hash = MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, new MurmurHash3.Hash128()).h1;
//        System.out.println(hash);
//        System.out.println((int)hash);

    }
}
