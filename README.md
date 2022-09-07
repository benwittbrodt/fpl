# fpl

## Endpoints

### Main
https://fantasy.premierleague.com/api/bootstrap-static/

Sections: 
   
* events
 * in db fpl.event 
 * sub table chip_plays, top_element
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

### Fixtures
https://fantasy.premierleague.com/api/fixtures/

Sections:

* fixtures
 * in db fpl.fixture
 * `id, gameweek, finished, finished_provisional, season_fixture, kickoff_time, minutes, provisional_start_time, started, team_a_id, team_a_score, team_h_id, team_h_score, team_h_difficulty, team_a_difficulty, pulse_id, season_name`
* stats
 * in db fpl.fixture_stat
 * `id, fixture_id, gameweek, fixture_num, stat_name, team_id, value, element_id, season_name` 
 
 
 
