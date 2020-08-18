package tr.com.mskr.sunshine22;

import android.app.LoaderManager;
import android.content.Context;
import android.content.CursorLoader;
import android.content.Intent;
import android.content.Loader;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import com.google.android.material.floatingactionbutton.FloatingActionButton;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.DividerItemDecoration;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;
import androidx.appcompat.widget.Toolbar;
import android.util.Log;
import android.view.View;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.Toast;

import tr.com.mskr.sunshine22.data.WeatherContract.*;
import tr.com.mskr.sunshine22.sync.SunshineService;

public class MainActivity extends AppCompatActivity implements
        LoaderManager.LoaderCallbacks<Cursor>,
        ListItemClickListener {

    public static final int      FORECAST_LOADER = 0;
    public static final String[] MAIN_FORECAST_PROJECTION = {
            WeatherEntry.COLUMN_DATE,
            WeatherEntry.COLUMN_MAX_TEMP,
            WeatherEntry.COLUMN_MIN_TEMP,
            WeatherEntry.COLUMN_WEATHER_ID,

    };
    private GreenAdapter          mAdapter;
    private RecyclerView          mWeatherList;
    private DividerItemDecoration mDividerItemDecoration;

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);

        setContentView(R.layout.activity_main);

        getLoaderManager().initLoader(FORECAST_LOADER, null, this);
        updateWeather();

        Toolbar toolbar                   = findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        mWeatherList                      = findViewById(R.id.rv_forecast);
        LinearLayoutManager layoutManager = new LinearLayoutManager(this);

        mWeatherList.setLayoutManager(layoutManager);
        mWeatherList.setHasFixedSize(true);
        mDividerItemDecoration            = new DividerItemDecoration(mWeatherList.getContext(), layoutManager.getOrientation());
        mWeatherList.addItemDecoration(mDividerItemDecoration);

        mAdapter                          = new GreenAdapter(this);
        mWeatherList.setAdapter(mAdapter);

        FloatingActionButton fab            = findViewById(R.id.fab);
        fab.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
            }
        });
    }

    @Override
    public Loader<Cursor> onCreateLoader(int i, Bundle bundle) {

        String locationSetting       = WeatherUtility.getPreferredLocation(this);
        String sortOrder             = WeatherEntry.COLUMN_DATE + " ASC";
        Uri    weatherForLocationUri = WeatherEntry.buildWeatherLocationWithStartDate(
                locationSetting,
                System.currentTimeMillis()
        );

        return new CursorLoader(this,
                weatherForLocationUri,
                MAIN_FORECAST_PROJECTION,
                null,
                null,
                sortOrder);
    }

    @Override
    public void onLoadFinished(Loader<Cursor> loader, Cursor cursor) {
        mAdapter.swapCursor(cursor);
    }

    @Override
    public void onLoaderReset(Loader<Cursor> loader) {
        mAdapter.swapCursor(null);
    }

    private void updateWeather(){
        Intent intent = new Intent(this, SunshineService.class);
        intent.putExtra(SunshineService.LOCATION_QUERY_EXTRA, WeatherUtility.getPreferredLocation(this));
        startService(intent);

//        FetchWeatherTask weatherTask = new FetchWeatherTask(this.getApplicationContext());
//        String           location    = WeatherUtility.getPreferredLocation(this);
//        weatherTask.execute(location);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {

        getMenuInflater().inflate(R.menu.menu_main, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {

        int id = item.getItemId();

        Context context = getApplicationContext();
        int duration    = Toast.LENGTH_SHORT;

        if (id == R.id.action_settings) {

            Intent intent = new Intent(MainActivity.this, SettingsActivity.class);
            startActivity(intent);

        } else if (id == R.id.action_help){
            Toast toast = Toast.makeText(context, "Help is clicked", duration);
            toast.show();

        } else if (id == R.id.action_refresh){

            updateWeather();
            getLoaderManager().restartLoader(FORECAST_LOADER, null, this);
        }

        return super.onOptionsItemSelected(item);
    }

    @Override
    public void onListItemClick(int clickedItemIndex) {

        Intent detailActivityIntent;

        Log.v("MainActivity.onCreate", "Item#"+Integer.toString(clickedItemIndex));




        detailActivityIntent = new Intent(MainActivity.this, DetailActivity.class);
        detailActivityIntent.putExtra(Intent.EXTRA_TEXT, "Detailed Weather Info");

        startActivity(detailActivityIntent);
    }
}