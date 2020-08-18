package tr.com.mskr.sunshine22.sync;

import android.app.IntentService;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Intent;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import tr.com.mskr.sunshine22.data.WeatherContract;
import tr.com.mskr.sunshine22.data.WeatherInformation;


/**
 * An {@link IntentService} subclass for handling asynchronous task requests in
 * a service on a separate handler thread.
 * <p>
 * TODO: Customize class - update intent actions and extra parameters.
 */
public class SunshineService extends IntentService {
    // TODO: Rename actions, choose action names that describe tasks that this
    // IntentService can perform, e.g. ACTION_FETCH_NEW_ITEMS
    public static final String ACTION_FOO = "tr.com.mskr.sunshine22.sync.action.FOO";
    public static final String ACTION_BAZ = "tr.com.mskr.sunshine22.sync.action.BAZ";

    public static final String LOCATION_QUERY_EXTRA = "lqe";
    private final String LOG_TAG = SunshineService.class.getSimpleName();
    private ContentResolver mResolver;

    public SunshineService() {
        super("SunshineService");
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        mResolver = this.getContentResolver();

        HttpURLConnection urlConnection   = null;
        BufferedReader reader          = null;
        String 		      forecastJsonStr = null;

        String locationQuery            = intent.getStringExtra(LOCATION_QUERY_EXTRA);
        String format                   = "json";
        String units                    = "metric";
        int numDays                     = 7;

        try {
            final String FORECAST_BASE_URL = "http://api.openweathermap.org/data/2.5/forecast/daily?";
            final String QUERY_PARAM       = "q";
            final String FORMAT_PARAM      = "mode";
            final String UNITS_PARAM       = "units";
            final String DAYS_PARAM        = "cnt";
            final String APPID_PARAM       = "APPID";

            Uri builtUri = Uri.parse(FORECAST_BASE_URL).buildUpon()
                    .appendQueryParameter(QUERY_PARAM , locationQuery)
                    .appendQueryParameter(FORMAT_PARAM, format)
                    .appendQueryParameter(UNITS_PARAM , units)
                    .appendQueryParameter(DAYS_PARAM  , Integer.toString(numDays))
                    .appendQueryParameter(APPID_PARAM , "1b3a6d183e0681e26f960c86ee271000")
                    .build();

            URL weatherURL = new URL(builtUri.toString());
            urlConnection  = (HttpURLConnection) weatherURL.openConnection();
            urlConnection.setRequestMethod("GET");
            urlConnection.connect();

            InputStream inputStream = urlConnection.getInputStream();
            StringBuffer buffer     = new StringBuffer();

            if (inputStream != null) {
                reader = new BufferedReader(new InputStreamReader(inputStream));

                String line;
                while ((line = reader.readLine()) != null) {
                    buffer.append(line + "\n");
                }
                if (buffer.length() != 0) {
                    forecastJsonStr = buffer.toString();
                }
            }
        } catch (IOException e) {
            Log.e("MainActivity", "Error ", e);
        } finally{
            if (urlConnection != null) {
                urlConnection.disconnect();
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (final IOException e) {
                    Log.e("MainActivity", "Error closing stream", e);
                }
            }
        }

        try {
            getWeatherDataFromJson(forecastJsonStr, locationQuery);
        } catch (JSONException e) {
            Log.e("FetchWeatherTask", e.getMessage(), e);
        }
        return;

//        if (intent != null) {
//            final String action = intent.getAction();
//            if (ACTION_FOO.equals(action)) {
//                final String param1 = intent.getStringExtra(EXTRA_PARAM1);
//                final String param2 = intent.getStringExtra(EXTRA_PARAM2);
//                handleActionFoo(param1, param2);
//            } else if (ACTION_BAZ.equals(action)) {
//                final String param1 = intent.getStringExtra(EXTRA_PARAM1);
//                final String param2 = intent.getStringExtra(EXTRA_PARAM2);
//                handleActionBaz(param1, param2);
//            }
//        }
    }

    private void getWeatherDataFromJson(String forecastJsonStr, String locationSetting)
            throws JSONException {

        final String OWM_LIST           = "list";

        final String OWM_CITY           = "city";
        final String OWM_CITY_NAME      = "name";
        final String OWM_COORD          = "coord";
        final String OWM_LATITUDE       = "lat";
        final String OWM_LONGITUDE      = "lon";

        final String OWM_WEATHER        = "weather";
        final String OWM_TEMPERATURE    = "temp";
        final String OWM_MAX            = "max";
        final String OWM_MIN            = "min";
        final String OWM_DESCRIPTION    = "main";
        final String OWM_DATETIME       = "dt";

        final String OWM_PRESSURE       = "pressure";
        final String OWM_HUMIDITY       = "humidity";
        final String OWM_WINDSPEED      = "speed";
        final String OWM_WIND_DIRECTION = "deg";

        final String OWM_WEATHER_ID     = "id";

        final int    SEC_TO_MILISEC     = 1000;

        JSONObject forecastJson  = new JSONObject(forecastJsonStr);
        JSONArray weatherArray  = forecastJson.getJSONArray(OWM_LIST);

        JSONObject cityJson      = forecastJson.getJSONObject(OWM_CITY);
        String     cityName      = cityJson.getString(OWM_CITY_NAME);

        JSONObject cityCoord     = cityJson.getJSONObject(OWM_COORD);
        double     cityLat       = cityCoord.getDouble(OWM_LATITUDE);
        double     cityLon       = cityCoord.getDouble(OWM_LONGITUDE);

        long locationId          = addLocation(locationSetting, cityName, cityLat, cityLon);

        for(int i = 0; i < weatherArray.length(); i++) {

            WeatherInformation wInfo             = new WeatherInformation();
            JSONObject         dayForecast       = weatherArray.getJSONObject(i);
            JSONObject         weatherObject     = dayForecast.getJSONArray(OWM_WEATHER).getJSONObject(0);
            JSONObject         temperatureObject = dayForecast.getJSONObject(OWM_TEMPERATURE);

            wInfo.locationID    = locationId;
            wInfo.dateTime      = dayForecast.getLong(OWM_DATETIME)*SEC_TO_MILISEC;
            wInfo.pressure      = dayForecast.getDouble(OWM_PRESSURE);
            wInfo.humidity      = dayForecast.getInt(OWM_HUMIDITY);
            wInfo.windSpeed     = dayForecast.getDouble(OWM_WINDSPEED);
            wInfo.windDirection = dayForecast.getDouble(OWM_WIND_DIRECTION);

            wInfo.description   = weatherObject.getString(OWM_DESCRIPTION);
            wInfo.weatherID     = weatherObject.getInt(OWM_WEATHER_ID);

            wInfo.high          = temperatureObject.getDouble(OWM_MAX);
            wInfo.low           = temperatureObject.getDouble(OWM_MIN);


            addWeather(wInfo);
        }
    }

    public long addLocation(String locationSetting, String cityName, double lat, double lon){

        Log.v("FetchWTask.addLocation", "Start");

        String[] projectedColumns = {WeatherContract.LocationEntry._ID};
        String   selectedString   = WeatherContract.LocationEntry.COLUMN_LOCATION_SETTING + "=?";
        String[] selectionArgs    = {locationSetting};
        Cursor locationCursor;
        long locationId;

        locationCursor = mResolver.query(WeatherContract.LocationEntry.CONTENT_URI,
                projectedColumns,
                selectedString,
                selectionArgs,
                null);

        if (locationCursor.moveToFirst()){
            int locationIdIndex = locationCursor.getColumnIndex(WeatherContract.LocationEntry._ID);
            locationId          = locationCursor.getLong(locationIdIndex);
        }
        else{
            Uri insertedUri;

            ContentValues locationValues = new ContentValues();
            locationValues.put(WeatherContract.LocationEntry.COLUMN_CITY_NAME       ,  cityName);
            locationValues.put(WeatherContract.LocationEntry.COLUMN_LOCATION_SETTING,  locationSetting);
            locationValues.put(WeatherContract.LocationEntry.COLUMN_COORD_LAT       ,  lat);
            locationValues.put(WeatherContract.LocationEntry.COLUMN_COORD_LONG      ,  lon);

            insertedUri = mResolver.insert(WeatherContract.LocationEntry.CONTENT_URI, locationValues);
            locationId  = ContentUris.parseId(insertedUri);
        }
        Log.v("FetchWTask.addLocation", "Stop");

        return locationId;
    }

    public void addWeather(WeatherInformation wInfo){

        ContentValues weatherValues = new ContentValues();

        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_LOC_KEY   , wInfo.locationID);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_DATE      , wInfo.dateTime);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_HUMIDITY  , wInfo.humidity);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_PRESSURE  , wInfo.pressure);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_WIND_SPEED, wInfo.windSpeed);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_DEGREES   , wInfo.windDirection);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_MAX_TEMP  , wInfo.high);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_MIN_TEMP  , wInfo.low);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_SHORT_DESC, wInfo.description);
        weatherValues.put(WeatherContract.WeatherEntry.COLUMN_WEATHER_ID, wInfo.weatherID);

        mResolver.insert(WeatherContract.WeatherEntry.CONTENT_URI, weatherValues);

    }

    /**
     * Handle action Foo in the provided background thread with the provided
     * parameters.
     */
    private void handleActionFoo(String param1, String param2) {
        // TODO: Handle action Foo
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Handle action Baz in the provided background thread with the provided
     * parameters.
     */
    private void handleActionBaz(String param1, String param2) {
        // TODO: Handle action Baz
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
