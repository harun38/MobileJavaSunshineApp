package tr.com.mskr.sunshine22;

import android.content.SharedPreferences;
import android.os.Bundle;
import android.preference.ListPreference;
import android.preference.Preference;
import android.preference.PreferenceFragment;
import android.preference.PreferenceManager;

public class SettingsFragment extends PreferenceFragment
        implements Preference.OnPreferenceChangeListener{

    @Override
    public void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);

        addPreferencesFromResource(R.xml.pref_general);

        Preference locationPreference = findPreference("location");
        Preference unitPreference     = findPreference("units");

        locationPreference.setOnPreferenceChangeListener(this);
        unitPreference.setOnPreferenceChangeListener(this);

        SharedPreferences loc_prefs =  PreferenceManager
                .getDefaultSharedPreferences(this.getActivity().getApplicationContext());
        onPreferenceChange(locationPreference, loc_prefs.getString(locationPreference.getKey(),""));

        SharedPreferences unit_prefs = PreferenceManager
                .getDefaultSharedPreferences(this.getActivity().getApplicationContext());
        onPreferenceChange(unitPreference, unit_prefs.getString(unitPreference.getKey(),""));
    }

    @Override
    public boolean onPreferenceChange(Preference preference, Object value) {

        String stringValue = value.toString();

        if (preference instanceof ListPreference) {

            ListPreference listPreference = (ListPreference) preference;
            int            prefIndex      = listPreference.findIndexOfValue(stringValue);

            if (prefIndex >= 0) {
                preference.setSummary(listPreference.getEntries()[prefIndex]);
            }
        } else {
            preference.setSummary(stringValue);
        }
        return true;
    }
}
