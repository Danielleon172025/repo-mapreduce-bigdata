package bigdata.mapreduce;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Analisis MapReduce del REPS (sedes de la red publica de salud de Bogota D.C.).
 *
 * Por cada sede el Mapper emite (clave, "1,e") donde e = 1 si la sede declara
 * atencion 24 horas en algun dia de la semana. Combiner y Reducer suman ambos
 * contadores, de modo que la salida es: clave \t total_sedes,sedes_24h.
 *
 * La clave depende del modo (parametro conteo.modo): localidad | subred | zona | nivel.
 */
public class ConteoSedes extends Configured implements Tool {

    public static final String PARAM_MODO = "conteo.modo";

    public enum Fila { LEIDAS, VALIDAS, INVALIDAS, ENCABEZADO }

    public static class SedeMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Charset ISO = Charset.forName("ISO-8859-1");
        private static final Pattern H24 =
            Pattern.compile("24\\s*hora|24h|00:00\\s*-\\s*24:00|0:00\\s*-\\s*23:59|00 a 24");

        private static final int COL_SUBRED = 4;
        private static final int COL_ZONA = 9;
        private static final int COL_LOCALIDAD_COD = 14;
        private static final int COL_LOCALIDAD_NOM = 15;
        private static final int COL_NIVEL = 31;
        private static final int COL_HORARIO_INI = 36;
        private static final int COL_HORARIO_FIN = 42;
        private static final int MIN_COLUMNAS = 43;

        private String modo;
        private final Text clave = new Text();
        private final Text valor = new Text();

        @Override
        protected void setup(Context context) {
            modo = context.getConfiguration().get(PARAM_MODO, "localidad");
        }

        @Override
        protected void map(LongWritable offset, Text linea, Context context)
                throws IOException, InterruptedException {
            context.getCounter(Fila.LEIDAS).increment(1);

            String texto = new String(linea.getBytes(), 0, linea.getLength(), ISO);
            String[] f = texto.split(";", -1);

            if (f[0].trim().equals("ID")) {
                context.getCounter(Fila.ENCABEZADO).increment(1);
                return;
            }
            if (f.length < MIN_COLUMNAS) {
                context.getCounter(Fila.INVALIDAS).increment(1);
                return;
            }

            String k;
            switch (modo) {
                case "subred":
                    k = f[COL_SUBRED].trim();
                    break;
                case "nivel":
                    k = "Nivel " + f[COL_NIVEL].trim();
                    break;
                case "zona":
                    k = f[COL_ZONA].trim();
                    break;
                default:
                    k = f[COL_LOCALIDAD_COD].trim() + " - " + f[COL_LOCALIDAD_NOM].trim();
            }

            StringBuilder horarios = new StringBuilder();
            for (int i = COL_HORARIO_INI; i <= COL_HORARIO_FIN; i++) {
                horarios.append(f[i]).append(' ');
            }
            int es24h = H24.matcher(horarios.toString().toLowerCase()).find() ? 1 : 0;

            clave.set(k);
            valor.set("1," + es24h);
            context.write(clave, valor);
            context.getCounter(Fila.VALIDAS).increment(1);
        }
    }

    /** Suma pares "total,24h". Sirve como Combiner y como Reducer. */
    public static class SumaReducer extends Reducer<Text, Text, Text, Text> {
        private final Text salida = new Text();

        @Override
        protected void reduce(Text clave, Iterable<Text> valores, Context context)
                throws IOException, InterruptedException {
            long total = 0;
            long h24 = 0;
            for (Text v : valores) {
                String[] p = v.toString().split(",");
                total += Long.parseLong(p[0]);
                h24 += Long.parseLong(p[1]);
            }
            salida.set(total + "," + h24);
            context.write(clave, salida);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Uso: ConteoSedes <entrada> <salida> <localidad|subred|zona|nivel>");
            return 2;
        }
        Configuration conf = getConf();
        conf.set(PARAM_MODO, args[2]);

        Job job = Job.getInstance(conf, "ConteoSedes-" + args[2]);
        job.setJarByClass(ConteoSedes.class);
        job.setMapperClass(SedeMapper.class);
        job.setCombinerClass(SumaReducer.class);
        job.setReducerClass(SumaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ConteoSedes(), args));
    }
}
