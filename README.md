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
 * Not really needed - settings shouldn't change, but might not include in final
* phases
 * in db fpl.phase
 * Not really needed - phases don't matter too much
* teams
 * in db fpl.team 
 * `team_id, draw, form, season_team_id, loss, name, played,
       points, position, short_name, strength, team_division,
       unavailable, win, strength_overall_home, strength_overall_away,
       strength_attack_home, strength_attack_away, strength_defence_home,
       strength_defence_away, pulse_id, season_name`
* total_players - single number, not needed in db
* elements
 * in db fpl.element 
 * Needs an update/historical setup
   * Also needs a sub-table/foreign keys for the stat updates
* element_stats
 * in db fpl.element_stat_type
 * `label, name`
* element_types
 * in db fpl.element_type
 * `id, plural_name, plural_name_short, singular_name, singular_name_short, squad_sheet, squad_min_play, squad_max_play, ui_shirt_specific, element_count`

### Fixtures - initial DB build done
https://fantasy.premierleague.com/api/fixtures/

Sections:

* fixtures
 * in db fpl.fixture
 * `id, gameweek, finished, finished_provisional, season_fixture, kickoff_time, minutes, provisional_start_time, started, team_a_id, team_a_score, team_h_id, team_h_score, team_h_difficulty, team_a_difficulty, pulse_id, season_name`
* stats
 * in db fpl.fixture_stat
 * `id, fixture_id, gameweek, fixture_num, stat_name, team_id, value, element_id, season_name` 
 
 
 
