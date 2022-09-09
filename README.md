# fpl

## Endpoints

### Main
https://fantasy.premierleague.com/api/bootstrap-static/
### TODO: add season names to tables
Sections: 
   
* events
 * in db fpl.event 
 * `id, name, deadline_time, average_entry_score, finished,
       data_checked, highest_scoring_entry, deadline_time_game_offset,
       highest_score, is_previous, is_current, is_next,
       cup_leagues_created, h2h_ko_matches_created, most_selected,
       most_transferred_in, top_element, transfers_made,
       most_captained, most_vice_captained, season_name`
 * related table chip_plays
 * `chip_name, num_played, event_id`
 * related table top_element	
 * `element_id, points,	event_id, season_name`
* game_settings
 * in db fpl.game_setting
* phases
 * in db fpl.phase
* teams
 * in db fpl.team 
* total_players - single number, not needed in db
* elements
 * in db fpl.element 
 * Needs an update/historical setup
* element_stats
 * in db fpl.element_stat
* element_types
 * in db fpl.element_type

### Fixtures - initial DB build done
https://fantasy.premierleague.com/api/fixtures/

Sections:

* fixtures
 * in db fpl.fixture
 * `id, gameweek, finished, finished_provisional, season_fixture, kickoff_time, minutes, provisional_start_time, started, team_a_id, team_a_score, team_h_id, team_h_score, team_h_difficulty, team_a_difficulty, pulse_id, season_name`
* stats
 * in db fpl.fixture_stat
 * `id, fixture_id, gameweek, fixture_num, stat_name, team_id, value, element_id, season_name` 
 
 
 
