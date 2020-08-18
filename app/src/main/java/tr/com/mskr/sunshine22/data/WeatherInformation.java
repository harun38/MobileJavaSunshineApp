package tr.com.mskr.sunshine22.data;

public class WeatherInformation {
    public long   locationID;	    // The foreign key into the location table.
    public long   dateTime;	    // Date, stored as long in milliseconds since the epoch

    public int    weatherID;	    // Weather id as returned by API, to identify the icon to be used
    public int    humidity;	    // Humidity representing percentage

    public double pressure;	    // Pressure representing percentage
    public double windSpeed;	    // Windspeed representing windspeed in mph.
    public double windDirection;	// Degrees are meteorological degrees (e.g, 0 is north, 180 is south)
    public double high;		    // Max temperature for the day
    public double low;		        // Min temperature for the day

    // Short description and long description of the weather, as provided by API.
    // e.g "clear" vs "sky is clear".

    public String description;
}