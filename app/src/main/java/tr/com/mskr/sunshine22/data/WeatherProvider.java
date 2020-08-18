package tr.com.mskr.sunshine22.data;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

import java.util.concurrent.TimeUnit;

import tr.com.mskr.sunshine22.data.WeatherContract.LocationEntry;
import tr.com.mskr.sunshine22.data.WeatherContract.WeatherEntry;

public class WeatherProvider extends ContentProvider {

    public static final int WEATHER                        = 100;
    public static final int WEATHER_WITH_LOCATION          = 101;
    public static final int WEATHER_WITH_LOCATION_AND_DATE = 102;
    public static final int LOCATION                       = 300;

    private static final UriMatcher      sUriMatcher = buildUriMatcher();
    private              WeatherDbHelper mOpenHelper;

    private static final SQLiteQueryBuilder sWeatherByLocationSettingQueryBuilder;

    static{
        sWeatherByLocationSettingQueryBuilder = new SQLiteQueryBuilder();

        // weather INNER JOIN location ON Weather.location_id = Location._id

        sWeatherByLocationSettingQueryBuilder.setTables(
                WeatherEntry.TABLE_NAME + " INNER JOIN " +
                        LocationEntry.TABLE_NAME +
                        " ON " + WeatherEntry.TABLE_NAME +
                        "." + WeatherEntry.COLUMN_LOC_KEY +
                        " = " + LocationEntry.TABLE_NAME +
                        "." + LocationEntry._ID);
    }

    //////////
    //// Selection strings to be used for querying the DB
    //// with special selection cases
    //////////

    // 1. LocationSetting & Day Selection String
    // (SQL command: location.location_setting = ? AND date = ?

    private static final String sLocationSettingAndDaySelection =
            LocationEntry.TABLE_NAME + "." +
                    LocationEntry.COLUMN_LOCATION_SETTING + " = ? AND " +
                    WeatherEntry.COLUMN_DATE + " = ? ";

    // 2. LocationSetting with Start Day Selection String
    // (SQL command: location.location_setting = ? AND date >= ?

    private static final String sLocationSettingWithStartDateSelection =
            LocationEntry.TABLE_NAME + "." +
                    LocationEntry.COLUMN_LOCATION_SETTING + " = ? AND " +
                    WeatherEntry.COLUMN_DATE + " >= ? ";

    // 3. LocationSetting Selection String
    // (SQL command:    location.location_setting = ?

    private static final String sLocationSettingSelection =
            LocationEntry.TABLE_NAME + "." +
                    LocationEntry.COLUMN_LOCATION_SETTING + " = ? ";

    public static UriMatcher buildUriMatcher(){

        UriMatcher matcher = new UriMatcher(UriMatcher.NO_MATCH);
        String authority   = WeatherContract.CONTENT_AUTHORITY;

        matcher.addURI(authority,  WeatherContract.PATH_LOCATION             , LOCATION);
        matcher.addURI(authority,  WeatherContract.PATH_WEATHER              , WEATHER);
        matcher.addURI(authority, WeatherContract.PATH_WEATHER + "/*"  , WEATHER_WITH_LOCATION);
        matcher.addURI(authority, WeatherContract.PATH_WEATHER + "/*/#", WEATHER_WITH_LOCATION_AND_DATE);

        return matcher;
    }

    public WeatherProvider() {
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final int match = sUriMatcher.match(uri);
        int rowsDeleted;

        if(selection == null)
            selection = "1";
        switch(match){
            case WEATHER:
                rowsDeleted = db.delete(WeatherEntry.TABLE_NAME, selection,
                        selectionArgs);;
                break;
            case LOCATION:
                rowsDeleted = db.delete(LocationEntry.TABLE_NAME, selection,
                        selectionArgs);
                break;
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        if(rowsDeleted != 0)
            getContext().getContentResolver().notifyChange(uri, null);

        return rowsDeleted;
    }

    @Override
    public String getType(Uri uri) {

        final int match = sUriMatcher.match(uri);

        switch (match){
            case WEATHER_WITH_LOCATION_AND_DATE: return WeatherEntry.CONTENT_ITEM_TYPE;
            case WEATHER_WITH_LOCATION         : return WeatherEntry.CONTENT_TYPE;
            case WEATHER                       : return WeatherEntry.CONTENT_TYPE;
            case LOCATION                      : return LocationEntry.CONTENT_TYPE;

            default:
                throw new UnsupportedOperationException("Unknown uri: "+ uri);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        final SQLiteDatabase db    = mOpenHelper.getWritableDatabase();
        final int            match = sUriMatcher.match(uri);
        Uri                  returnUri;

        switch(match){
            case LOCATION: {
                long _id = db.insert(LocationEntry.TABLE_NAME, null, values);

                if (_id > 0){
                    returnUri = LocationEntry.buildLocationUri(_id);
                }
                else{
                    throw new android.database.SQLException("Failed to insert row into" + uri);
                }

            } break;
            case WEATHER:{
                long _id = db.insert(WeatherEntry.TABLE_NAME, null, values);

                if (_id > 0){
                    returnUri = WeatherEntry.buildWeatherUri(_id);
                }
                else{
                    throw new android.database.SQLException("Failed to insert row into" + uri);
                }
            } break;
            default:{
                throw new UnsupportedOperationException("Unknown uri:" + uri);
            }
        }
        getContext().getContentResolver().notifyChange(uri,null);

        return returnUri;
    }

    @Override
    public boolean onCreate() {
        mOpenHelper = new WeatherDbHelper(getContext());
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        final SQLiteDatabase db    = mOpenHelper.getReadableDatabase();
        final int            match = sUriMatcher.match(uri);
        Cursor               retCursor;

        switch(match){
            case LOCATION: {
                retCursor = db.query(LocationEntry.TABLE_NAME,
                        projection,
                        selection,
                        selectionArgs,
                        null,
                        null,
                        sortOrder);
            } break;

            case WEATHER: {
                retCursor = db.query(WeatherEntry.TABLE_NAME,
                        projection,
                        selection,
                        selectionArgs,
                        null,
                        null,
                        sortOrder);
            } break;

            case WEATHER_WITH_LOCATION_AND_DATE:{

                String locationSetting = uri.getPathSegments().get(1);
                String dateStr         = uri.getPathSegments().get(2);

                retCursor = sWeatherByLocationSettingQueryBuilder.query(db,
                        projection,
                        sLocationSettingAndDaySelection,
                        new String[]{locationSetting,dateStr},
                        null,
                        null,
                        sortOrder);

            } break;

            case WEATHER_WITH_LOCATION:{

                String locationSetting = uri.getPathSegments().get(1);
                String dateStr         = uri.getQueryParameter(WeatherEntry.COLUMN_DATE);

                if (dateStr != null && dateStr.length() > 0){
                    selection     = sLocationSettingWithStartDateSelection;
                    selectionArgs = new String[]{locationSetting, dateStr};
                }
                else{
                    selection     = sLocationSettingSelection;
                    selectionArgs = new String[]{locationSetting};
                }

                retCursor = sWeatherByLocationSettingQueryBuilder.query(db,
                        projection,
                        selection,
                        selectionArgs,
                        null,
                        null,
                        sortOrder);
            } break;

            default:{
                throw new UnsupportedOperationException("Unknown uri:" + uri);
            }
        }
        retCursor.setNotificationUri(getContext().getContentResolver(), uri);
        return retCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        final SQLiteDatabase db = mOpenHelper.getWritableDatabase();
        final int match = sUriMatcher.match(uri);
        int rowsUpdated;

        switch(match){
            case WEATHER:
                normalizeDate(values);
                rowsUpdated = db.update(WeatherEntry.TABLE_NAME, values, selection,
                        selectionArgs);
                break;
            case LOCATION:
                rowsUpdated = db.update(LocationEntry.TABLE_NAME, values, selection,
                        selectionArgs);
                break;
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }

        if(rowsUpdated != 0)
            getContext().getContentResolver().notifyChange(uri, null);

        return rowsUpdated;
    }

    public static void normalizeDate(ContentValues cValues) {
        long date = cValues.getAsLong(WeatherEntry.COLUMN_DATE);
        long daysSinceEpoch = elapsedDaysSinceEpoch(date);
        long millisFromEpochToTodayAtMidnightUtc = daysSinceEpoch * DAY_IN_MILLIS;
        cValues.put(WeatherEntry.COLUMN_DATE, millisFromEpochToTodayAtMidnightUtc);
    }

    /* Milliseconds in a day */
    public static final long DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1);
    private static long elapsedDaysSinceEpoch(long utcDate) {
        return TimeUnit.MILLISECONDS.toDays(utcDate);
    }
}
