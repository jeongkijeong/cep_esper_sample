package com.spiri.common.util;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    private static Properties properties = null;

    public static String replace(String target, List<Map<String, String>> variableList) {
        if (target == null || variableList == null)
            return null;

        String KEY = "KEY";
        String VAL = "VAL";

        String replaced = target;

        for (Map<String, String> m : variableList) {
            String key = m.get(KEY);
            String val = m.get(VAL);

            if (key == null || val == null)
                continue;

            replaced = replaced.replace(key, val);
        }

        return replaced;
    }

    public static List<String> split(String target, int size) {
        List<String> list = new ArrayList<>();

        int len = target.length();
        if (len > size) {
            int stt = 0;
            int end = stt + size;

            String sb;

            boolean flag = true;

            while (flag) {
                if (end >= len) {
                    flag = false;
                    end = len;
                }

                sb = target.substring(stt, end);
                list.add(sb);

                stt = end;
                end = stt + size;
            }
        } else {
            list.add(target);
        }

        return list;
    }

    public static String objectToJsonStr(Object object) {
        if (object == null) {
            return null;
        }

        String result = null;
        try {
            Gson gson = new GsonBuilder().create();
            result = gson.toJson(object);
        } catch (Exception e) {
            e.getStackTrace();
        }

        return result;
    }

    public static <T> Object jsonStrToObject(String result, Class<T> classType) {
        Object jsonObj = null;

        if (result == null) {
            return jsonObj;
        }

        try {
            Gson gson = new GsonBuilder().create();
            jsonObj = gson.fromJson(result, classType);
        } catch (Exception e) {
            logger.error("", e);
        }

        return jsonObj;
    }

    public static Map<String, Object> jsonStrToMap(String result) {
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        if (result == null) {
            return null;
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
            };

            jsonMap = mapper.readValue(result, typeRef);
        } catch (Exception e) {
            logger.error("", e);
            return null;
        }

        return jsonMap;
    }


    public static int getProcessId() {
        int processId = -1;

        String name = ManagementFactory.getRuntimeMXBean().getName();
        if (name != null) {
            processId = Integer.valueOf(name.split("\\@")[0]);
        }

        return processId;
    }

    public static String fileToString(String path) {
        String result = "";

        File file = new File(path);
        if (!file.exists()) {
            System.out.println("file not exist " + path);

            return result;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file));) {
            StringBuilder sb = new StringBuilder();

            String temp;
            while ((temp = br.readLine()) != null) {
                sb.append(temp);
            }

            result = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public static String nvl(String target, String replace) {
        if (target == null) {
            return replace;
        }

        return target;
    }

    public static Object nvl(Object target, Object replace) {
        if (target == null) {
            return replace;
        }

        return target;
    }

    public static String nvl2(String target, String replace1, String replace2) {
        if (target == null) {
            return replace1;
        }

        return replace2;
    }

    public static Object nvl2(Object target, Object replace1, Object replace2) {
        if (target == null) {
            return replace1;
        }

        return replace2;
    }

    public static String rpad(String target, int size, String pad){
        if (target == null) {
            return "";
        }

        String convert = String.format("%-" + size + "s", target).replace(" ", pad);
        return convert;
    }

    public static String lpad(String target, int size, String pad){
        if (target == null) {
            return "";
        }

        String convert = String.format("%"  + size + "s", target).replace(" ", pad);
        return convert;
    }

    public static String currentDate(String format) {
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern(format));

        return date;
    }

    public static int loadProperties(String path) {
        int result = -1;

        File file = new File(path);
        if (!file.exists()) {
            return result;
        }

        try (FileInputStream fis = new FileInputStream(path)) {
            properties = new Properties();
            properties.load(fis);

            result = 0;
        } catch (Exception e) {
            e.getStackTrace();
        }

        return result;
    }

    public static int loadLogConfigs(String path) {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();

        try {
            configurator.doConfigure(path);
        } catch (JoranException e) {
            e.printStackTrace();
        }

        return 0;
    }

    public static String getProterty(String key) {
        String value = null;

        if (properties != null) {
            value = (String) nvl(properties.get(key), "");
        }

        return value;
    }

    public static Object jsonFileToObject(String path) {
        String string = fileToString(path);

        Object object = null;
        try {
            Gson gson = new GsonBuilder().create();
            object = gson.fromJson(string, new HashMap<String, Object>().getClass());
        } catch (Exception e) {
            logger.error("", e);
        }

        return object;
    }

    public static Map<String, Object> jsonFileToMap(String path) {
        Map<String, Object> jsonMap = null;

        try {
            jsonMap = jsonStrToMap(fileToString(path));
        } catch (Exception e) {
            logger.error("", e);
        }

        return jsonMap;
    }

    public static Properties mapToProperties(Map<String, Object> map) {
        Properties properties = new Properties();

        for (String key : map.keySet()) {
//            properties.setProperty(key, (String) map.get(key));
            properties.setProperty(key, map.get(key).toString());
        }

        return properties;
    }

    public static void mergeFile(String sourceFilePath, String targetFilePath) {
        File sourceFile = new File(sourceFilePath);
        File targetFile = new File(targetFilePath);

        try {
            String line = "";

            if (targetFile.exists() == true) {  // 대상파일 존재시 내용추가.
                FileWriter fileWriter = new FileWriter(targetFile, true);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFile)));
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

                int count = 0;
                while ((line = bufferedReader.readLine()) != null) {
                    if (count == 0) {
                        count++;

                        continue;
                    }

                    bufferedWriter.write(line);
                    bufferedWriter.write("\n");
                }

                bufferedWriter.flush();
                bufferedReader.close();
                bufferedWriter.close();
            } else {
                sourceFile.renameTo(targetFile);
            }

            sourceFile.delete();
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public static List<Map<String, String>> matchedList (List<Map<String, String>> sourceList, List<Map<String, String>> targetList, String key) {
        List<Map<String, String>> t = sourceList.stream().filter(target ->
                targetList.stream().anyMatch(source -> source.get(key).equals(target.get(key)))).collect(Collectors.toList());

        return t;
    }

    public static List<Map<String, String>> removedList (List<Map<String, String>> sourceList, List<Map<String, String>> targetList, String key) {
        sourceList.removeIf(target -> targetList.stream().anyMatch(source -> source.get(key).equals(target.get(key))));

        return sourceList;
    }

    public static Map<String, Object> putAll(Map<String, Object> target, Map<String, Object> source){
        target.putAll(source);
        return target;
    }

    /**
     * <pre>
     * 현재 날짜(또는 일자,시간)를 다양한 포멧으로 리턴한다.
     * iCase
     *  1 : yyyyMMddHHmmss
     *  2 : yyyyMMddHHmm
     *  3 : dd
     *  4 : yyyyMMdd
     *  5 : yyyy/MM/dd HH:mm:ss
     *  6 : MM/dd HH:mm:ss
     *  7 : HH
     *  8 : mm
     *  9 : HHmm
     * </pre>
     * @param iCase 숫자에 따라서 시간 포맷이 달라진다.
     * */
    static public String getTime(int iCase) {
        Calendar cal = Calendar.getInstance(new Locale("Korean", "Korea"));
        SimpleDateFormat df = null;

        switch (iCase) {
            case 1: df = new SimpleDateFormat("yyyyMMddHHmmss"); break;
            case 2: df = new SimpleDateFormat("yyyyMMddHHmm");   break;
            case 3: df = new SimpleDateFormat("yyyyMMddHH");     break;
            case 4: df = new SimpleDateFormat("yyyyMMdd");       break;
            case 5: df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"); break;
            case 6: df = new SimpleDateFormat("MM/dd HH:mm:ss"); break;
            case 7: df = new SimpleDateFormat("HH");             break;
            case 8: df = new SimpleDateFormat("mm");             break;
            case 9: df = new SimpleDateFormat("mmss");           break;
            case 10: df = new SimpleDateFormat("yyyyMM");        break;
            default: break;
        }

        return df.format(cal.getTime());
    }

    public static String convDateFormat(String time, String sourceFormat, String targetFormat) {
        String convDateFormat = "";

        try {
            if (time != null) {
                time = time.substring(0, time.length() - 1) + "0";
            }

            DateFormat sourceDateFormat = new SimpleDateFormat(sourceFormat);
            Date date = sourceDateFormat.parse(time);

            DateFormat targetDateFormat = new SimpleDateFormat(targetFormat);
            convDateFormat = targetDateFormat.format(date);
        } catch (Exception e) {
            logger.error("", e);
        }

        return convDateFormat;
    }

    static public int makeDirectory(String directoryPath) {
        int retv = -1;

        Path path = Paths.get(directoryPath);

        try {
            File directory = path.toFile();
            if (directory.exists() == false) {
                boolean t = directory.mkdirs();
                if (t == true) {
                    logger.info("success make directory {}", directory.getAbsolutePath());
                    retv = 0;
                } else {
                    logger.info("failure make directory {}", directory.getAbsolutePath());
                }
            } else {
                retv = 0;
            }
        } catch (Exception e) {
            logger.error("", e);
        }

        return retv;
    }

    public static int writeToFile(String basePath, String fileName, String data) {
        int retv = -1;

        retv = makeDirectory(basePath);
        if (retv < 0) {
            return -2;  // 디렉토리생성 실패.
        }

        Path path = Paths.get(basePath, fileName);
        File file = path.toFile();

        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(data.getBytes());

            retv = 0;

            logger.info("success write file {}", path.toFile().getAbsolutePath());
        } catch (Exception e) {
            logger.error("", e);
        }

        return retv;
    }

}
