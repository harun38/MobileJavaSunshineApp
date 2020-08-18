package tr.com.mskr.sunshine22;

import androidx.recyclerview.widget.RecyclerView;
import android.view.View;
import android.widget.TextView;

public class ForecastViewHolder extends RecyclerView.ViewHolder
implements View.OnClickListener{

    private  ListItemClickListener mListener;
    public   TextView              listItemForecastView;

    public ForecastViewHolder(View itemView, ListItemClickListener listener) {
        super(itemView);
        listItemForecastView = (TextView) itemView.findViewById(R.id.tv_item_forecast);
        mListener            = listener;
        itemView.setOnClickListener(this);
    }

    void bind(String s) {

        listItemForecastView.setText(s);
    }

    @Override
    public void onClick(View view) {

        int clickedPosition = getAdapterPosition();
        mListener.onListItemClick(clickedPosition);
    }
}