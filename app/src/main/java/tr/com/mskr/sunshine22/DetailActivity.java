package tr.com.mskr.sunshine22;

import android.content.Intent;
import androidx.appcompat.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.widget.TextView;

public class DetailActivity extends AppCompatActivity {

    private TextView mDetailInfoView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        Intent   intentThatStartedThisActivity;
        String   forecastStr;

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail);

        mDetailInfoView               = (TextView) findViewById(R.id.tv_detail_forecast);
        intentThatStartedThisActivity = getIntent();

        if (intentThatStartedThisActivity.hasExtra(Intent.EXTRA_TEXT)){

            forecastStr = intentThatStartedThisActivity.getStringExtra(Intent.EXTRA_TEXT);
            Log.v("DetailActivity.onCreate", forecastStr);

            mDetailInfoView.setText(forecastStr);
        }
        else{
            mDetailInfoView.setText("NO WEATHER DATA");
        }
    }
}
