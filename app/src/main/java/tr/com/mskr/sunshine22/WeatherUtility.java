package tr.com.mskr.sunshine22;

import android.content.Context;
import android.content.SharedPreferences;
import android.preference.PreferenceManager;
import java.text.SimpleDateFormat;

public class WeatherUtility {

    public static String getPreferredLocation(Context context) {

        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(context);
        return prefs.getString("location", "94043");
    }

    public static String getReadableDateString(long time){

        SimpleDateFormat shortenedDateFormat = new SimpleDateFormat("EEE MMM dd");
        return shortenedDateFormat.format(time);
    }
    public static String formatHighLows(double high, double low, String unit) {

        String   DEGREE   = "\u00b0";

        if (unit.equals("imperial")){

            high = convertIntoFahrenheit(high);
            low  = convertIntoFahrenheit(low);
        }

        long roundedHigh  = Math.round(high);
        long roundedLow   = Math.round(low);

        String highLowStr = roundedHigh + DEGREE + "/" + roundedLow + DEGREE;

        return highLowStr;
    }
    private static double convertIntoFahrenheit(double temperature){

        return (temperature*1.8) + 32;
    }
}
