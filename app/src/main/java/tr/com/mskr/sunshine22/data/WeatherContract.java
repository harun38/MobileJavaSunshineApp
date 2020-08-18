package tr.com.mskr.sunshine22.data;

import android.content.ContentResolver;
import android.content.ContentUris;
import android.net.Uri;
import android.provider.BaseColumns;

public class WeatherContract {

    public static final String CONTENT_AUTHORITY = "tr.com.mskr.sunshine22";
    public static final Uri    BASE_CONTENT_URI  = Uri.parse("content://" + CONTENT_AUTHORITY);
    public static final String PATH_WEATHER      = "weather";
    public static final String PATH_LOCATION     = "location";

    ///////////////////////////////////////////////////////////////////////////////
    //
    // LocationEntry (Inner class) Class that defines the contents of the location table
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final class LocationEntry implements BaseColumns {

        public static final Uri    CONTENT_URI       = BASE_CONTENT_URI.buildUpon()
                .appendPath(PATH_LOCATION)
                .build();
        public static final String CONTENT_TYPE      = ContentResolver.CURSOR_DIR_BASE_TYPE + "/" + CONTENT_AUTHORITY + "/" + PATH_LOCATION;
        public static final String CONTENT_ITEM_TYPE = ContentResolver.CURSOR_ITEM_BASE_TYPE + "/" + CONTENT_AUTHORITY + "/" + PATH_LOCATION;

        public static final String TABLE_NAME              = "location";

        // The location setting string is what will be sent to openweathermap
        // as the location query.
        public static final String COLUMN_LOCATION_SETTING = "location_setting";

        // Human readable location string, provided by the API.  Because for styling,
        // "Mountain View" is more recognizable than 94043.
        public static final String COLUMN_CITY_NAME        = "city_name";
        public static final String COLUMN_COORD_LAT        = "coord_lat";
        public static final String COLUMN_COORD_LONG       = "coord_long";

        public static Uri buildLocationUri(long id) {
            return ContentUris.withAppendedId(CONTENT_URI, id);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // WeatherEntry (Inner class) Class that defines the contents of the weather table
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final class WeatherEntry implements BaseColumns {

        public static final Uri    CONTENT_URI       = BASE_CONTENT_URI.buildUpon()
                .appendPath(PATH_WEATHER)
                .build();
        public static final String CONTENT_TYPE      = ContentResolver.CURSOR_DIR_BASE_TYPE + "/" + CONTENT_AUTHORITY + "/" + PATH_WEATHER;
        public static final String CONTENT_ITEM_TYPE = ContentResolver.CURSOR_ITEM_BASE_TYPE + "/" + CONTENT_AUTHORITY + "/" + PATH_WEATHER;

        public static final String TABLE_NAME               = "weather";

        // Column with the foreign key into the location table.
        public static final String COLUMN_LOC_KEY           = "location_id";
        // Date, stored as long in milliseconds since the epoch
        public static final String COLUMN_DATE              = "date";
        // Weather id as returned by API, to identify the icon to be used
        public static final String COLUMN_WEATHER_ID        = "weather_id";

        // Short description and long description of the weather, as provided by API.
        // e.g "clear" vs "sky is clear".
        public static final String COLUMN_SHORT_DESC        = "short_desc";

        // Min and max temperatures for the day (stored as floats)
        public static final String COLUMN_MIN_TEMP          = "min";
        public static final String COLUMN_MAX_TEMP          = "max";

        // Humidity is stored as a float representing percentage
        public static final String COLUMN_HUMIDITY          = "humidity";

        // Humidity is stored as a float representing percentage
        public static final String COLUMN_PRESSURE          = "pressure";

        // Windspeed is stored as a float representing windspeed  mph
        public static final String COLUMN_WIND_SPEED        = "wind";

        // Degrees are meteorological degrees (e.g, 0 is north, 180 is south).  Stored as floats.
        public static final String COLUMN_DEGREES           = "degrees";

        public static Uri buildWeatherUri(long id) {
            return ContentUris.withAppendedId(CONTENT_URI, id);
        }

        // Method for building a Location URI (Type 4)
        public static Uri buildWeatherLocation(String locationSetting) {
            // TO DO
            return null;
        }

        // Method for building a Location and Date URI (Type 3)
        public static Uri buildWeatherLocationWithDate(String locationSetting, long date) {
            return CONTENT_URI.buildUpon()
                    .appendPath(locationSetting)
                    .appendPath(Long.toString(date))
                    .build();
        }

        // Method for building a Location and Date URI with Start Date as a Query Param (Type 5)
        public static Uri buildWeatherLocationWithStartDate(String locationSetting, long startDate) {

            return CONTENT_URI.buildUpon()
                    .appendPath(locationSetting)
                    .appendQueryParameter(COLUMN_DATE, Long.toString(startDate))
                    .build();
        }

        // Based on our URI definitions, the first path of the URI
        // will be the "Location" information. Therefore, we are getting the first
        // path segment to get the location information

        public static String getLocationSettingFromUri(Uri uri) {
            return uri.getPathSegments().get(1);
        }

        // Based on our URI definitions, the second path of the URI
        // will be the "Date" information. Therefore, we are getting the second
        // path segment to get the date information

        public static long getDateFromUri(Uri uri) {
            return Long.parseLong(uri.getPathSegments().get(2));
        }

        public static long getStartDateFromUri(Uri uri) {
            String dateString = uri.getQueryParameter(COLUMN_DATE);
            if (null != dateString && dateString.length() > 0)
                return Long.parseLong(dateString);
            else
                return 0;
        }
    }
}