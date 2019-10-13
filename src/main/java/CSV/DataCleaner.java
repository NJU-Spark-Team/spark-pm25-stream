package CSV;

import org.bson.Document;

/**
 * Take responsibility of data cleaning.
 */
public class DataCleaner {

    private String cityName;
    private String lastPM = "";
    private String lastHumidity = "";
    private String lastTemperature = "";
    private String lastDewp = "";
    private String lastLws = "";

    public DataCleaner(String city){
        this.cityName = city;
    }

    public boolean clean(Document document){
        if (document.get("name", String.class) != this.cityName){
            return false;
        }
        if (!isDigit(document.get("year", String.class)) ||
                !isDigit(document.get("month", String.class)) ||
                !isDigit(document.get("day", String.class)) ||
                !isDigit(document.get("hour", String.class))){
            return false;
        }
        if (!isDigit(document.get("PM", String.class))){
            if (lastPM == ""){
                return false;
            }
            document.put("PM", lastPM);
        }
        else {
            lastPM = document.get("PM", String.class);
        }

        if (!isDigit(document.get("humidity", String.class))){
            if (lastHumidity == ""){
                return false;
            }
            document.put("humidity", lastHumidity);
        }
        else {
            lastHumidity = document.get("humidity", String.class);
        }

        if (!isDigit(document.get("temperature", String.class), true)){
            if (lastTemperature == ""){
                return false;
            }
            document.put("temperature", lastTemperature);
        }
        else {
            lastTemperature = document.get("temperature", String.class);
        }

        if (!isDigit(document.get("dewp", String.class))){
            if (lastDewp == ""){
                return false;
            }
            document.put("dewp", lastDewp);
        }
        else {
            lastDewp = document.get("dewp", String.class);
        }

        return true;
    }

    private boolean isDigit(String s){
        return isDigit(s, false);
    }

    private boolean isDigit(String s, boolean flag){
        if (s == null){
            return false;
        }
        if (s.length() == 0){
            return false;
        }
        if (s.charAt(0) == '-'){
            return isDigit(s.substring(1));
        }
        for (int i = 0; i < s.length(); i ++){
            if (s.charAt(i) == '.' && flag){
                return isDigit(s.substring(i + 1));
            }
            if (s.charAt(i) < '0' || s.charAt(i) > '9'){
                return false;
            }
        }
        return true;
    }
}
