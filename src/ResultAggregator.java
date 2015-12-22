import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;

/**
 * Created by user on 10/10/2015.
 */
public class ResultAggregator {

    private static final String SEP = "\\";

    private ExecutorService executor = Executors.newCachedThreadPool();


    public static void main(String[] args) {

        String currentPath = System.getProperty("user.dir") + SEP;

        JFrame frame = new JFrame("Transformer");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JLabel label1 = new JLabel("DIR: " + currentPath);
        frame.add(label1);

        frame.pack();
        frame.setVisible(true);
        new ResultAggregator(currentPath);
    }

    public ResultAggregator(String dirPath) {
        List<String> fileNames = getFileNames(dirPath);
        for (String fileName : fileNames) {
            executor.submit(new AggregationTask(dirPath, fileName));
        }

    }

    private List<String> getFileNames(String folderPath) {
        File folder = new File(folderPath);
        File[] listOfFiles = folder.listFiles();
        List<String> fileNames = new ArrayList<>();
        System.out.println("Files at: " + folderPath);
        FileNameExtensionFilter extFilter = new FileNameExtensionFilter("Filter for input files: ", ".csv");
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile() && extFilter.accept(listOfFiles[i])) {
                fileNames.add(listOfFiles[i].getName());
            }
        }
        return fileNames;
    }

    private static void generateCSVFile(String fileName, TreeMap<String, Set<String>> patternToSong) throws IOException {
        FileWriter fw = null;
        fw = new FileWriter(fileName);

        fw.append("Count,Pattern,Songs\n");
        String key = "", val = "";
        for (Map.Entry association : patternToSong.entrySet()) {
            int sizeOfCurrent = ((Set<String>) association.getValue()).size();
//            System.out.println("Pattern:\n\t" + association.getKey() + "\n\tfound in songs: " + "\n\t" + association.getValue());
            key = association.getKey().toString();
            val = association.getValue().toString();
            String size = String.valueOf(sizeOfCurrent);
            fw.append(size).append(",").append(key).append(",").append(val).append("\n");
        }

    }

    class AggregationTask implements Runnable {
        private String path;
        private String fileName;

        public AggregationTask(String path, String fileName) {
            this.path = path;
            this.fileName = fileName;
        }

        @Override
        public void run() {
            System.out.println("Thread " + Thread.currentThread().getId() + " aggregating document");
            methodThatDoesAll(path, fileName);
        }

        private void methodThatDoesAll(String path, String fileName) {
            System.out.println("Thread: " + Thread.currentThread().getId() + " starts to process file: " + path + fileName);
            TreeMap<String, Set<String>> patternToSong = new TreeMap<>();
            BufferedReader bufReader = null;
            try {
                bufReader = new BufferedReader(new FileReader(path + fileName));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            String line = "";
            try {
                //ignore first line
                bufReader.readLine();
                while ((line = bufReader.readLine()) != null) {
                    String[] parts = line.split(",");
                    String song1 = "", song2 = "", pattern = "";
                    pattern = parts[parts.length - 1];

                    if (parts.length == 5) {
                        song1 = parts[0];
                        song2 = parts[2];
                    } else if (parts.length == 6) {
                        if (parts[0].toLowerCase().contains("nu")) {
                            song1 = parts[0] + parts[1];
                            song2 = parts[3];
                        } else if (parts[2].toLowerCase().contains("nu")) {
                            song1 = parts[0];
                            song2 = parts[2] + parts[3];
                        }
                    } else if (parts.length == 7) {
                        song1 = parts[0] + parts[1];
                        song2 = parts[3] + parts[4];
                    }

                    if (patternToSong.get(pattern) == null) {
                        Set<String> songs = new HashSet<>();
                        songs.add(song1);
                        songs.add(song2);
                        patternToSong.put(pattern, songs);
                    } else {
                        patternToSong.get(pattern).add(song1);
                        patternToSong.get(pattern).add(song2);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            String outPathStr = path + "output\\";
            System.out.println("Output path:" + outPathStr);
            File outDir = new File(outPathStr);

            if (!outDir.exists()) {
                outDir.mkdir();
            }

            try {
                generateCSVFile(outPathStr + "arranged_" + fileName, patternToSong);
            } catch (IOException e) {
            }
            System.out.println("Thread: " + Thread.currentThread().getId() + " finished processing file: " + path + fileName);
        }
    }
}
