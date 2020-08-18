package tr.com.mskr.sunshine22;

import android.content.Context;
import android.database.Cursor;
import androidx.recyclerview.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

public class GreenAdapter extends RecyclerView.Adapter<ForecastViewHolder> {

    private Cursor                mCursor;
    private ListItemClickListener mOnClickListener;

    public GreenAdapter(ListItemClickListener listener) {

        mOnClickListener = listener;
    }

    @Override
    public ForecastViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {

        Context        context                         = parent.getContext();
        int            layoutIdForListItem             = R.layout.forecast_list_item;
        LayoutInflater inflater                        = LayoutInflater.from(context);
        boolean        shouldAttachToParentImmediately = false;

        View view = inflater.inflate(layoutIdForListItem, parent, shouldAttachToParentImmediately);
        ForecastViewHolder viewHolder = new ForecastViewHolder(view, mOnClickListener);

        return viewHolder;
    }

    @Override
    public void onBindViewHolder(ForecastViewHolder holder, int position) {
        mCursor.moveToPosition(position);

        long   dateInMiliSec     = mCursor.getLong(0);
        double highTempInCelcius = mCursor.getDouble(1);
        double lowTempInCelcius  = mCursor.getDouble(2);
        String weatherDesc       = mCursor.getString(3);

        String highLowTemperature = WeatherUtility.formatHighLows(highTempInCelcius, lowTempInCelcius, "Celcius");
        String dateString         = WeatherUtility.getReadableDateString(dateInMiliSec);
        String weatherSummary     = dateString + " - " + weatherDesc + " - " + highLowTemperature;

        holder.bind(weatherSummary);
    }

    @Override
    public int getItemCount() {

        if (mCursor == null){
            return 0;
        }
        else{
            return mCursor.getCount();
        }
    }

    public void swapCursor(Cursor newCursor){
        mCursor = newCursor;
        notifyDataSetChanged();
    }
}