Feature: REST
  As a tester
  I want to invoke system's REST API
  In order to test system functionality

  Scenario: Get Star Wars character
    When I get character by id 1
    Then the response status is 200
    And the response is:
      | name           | height | mass | hair_color | skin_color | eye_color | birth_year | gender | homeworld                       | url                            |
      | Luke Skywalker | 172    | 77   | blond      | fair       | blue      | 19BBY      | male   | https://swapi.co/api/planets/1/ | https://swapi.co/api/people/1/ |

    When I get character by id 2
    Then the response status is 200
    And the response is:
      | name  | height | mass | hair_color | skin_color | eye_color | birth_year | gender | homeworld                       | url                            |
      | C-3PO | 167    | 75   | n/a        | gold       | yellow    | 112BBY     | n/a    | https://swapi.co/api/planets/1/ | https://swapi.co/api/people/2/ |
