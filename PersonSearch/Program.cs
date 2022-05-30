// See https://aka.ms/new-console-template for more information

using Bogus;

using Microsoft.Data.Sqlite;

using System.Diagnostics;

string databasePath = $"""{AppDomain.CurrentDomain.BaseDirectory}data\""";
string databaseFileName = "people.sqlite";
string databaseFullPath = Path.Combine(databasePath, databaseFileName);
int numOfTestUsersToGenerate = 100_000;

Directory.CreateDirectory(databasePath);
//Check if database file exists
if (!System.IO.File.Exists(databaseFullPath))
{
    CreateDatabase(databaseFullPath, numOfTestUsersToGenerate);
}

using (var connection = new SqliteConnection($"Data Source={databaseFullPath}"))
{
    connection.Open();
    using (SqliteCommand command = connection.CreateCommand())
    {
        command.CommandText = """
            WITH split (item, query) AS (SELECT '', LOWER(REPLACE(TRIM(@query), ',', ' ')) || ' '
                    UNION ALL
                    SELECT SUBSTR(query, 1, INSTR(query, ' ') - 1),
                        SUBSTR(query, INSTR(query, ' ') + 1)
                    FROM split
                    WHERE query != ''),
                 query_parts AS (SELECT item AS value FROM split WHERE item != '')
            SELECT person_id,
                   person_name,
                   (SELECT COUNT(*) AS match_count
                    FROM (SELECT value
                          FROM query_parts
                          WHERE LOWER(first_name) LIKE value || '%'
                             OR LOWER(middle_name) LIKE value || '%'
                             OR LOWER(last_name) LIKE value || '%'
                             OR LOWER(preferred_name) LIKE value || '%')) AS match_count
            from person
            where match_count > 0
            order by match_count desc limit 20
            """;
        command.Parameters.Add("@query", SqliteType.Text);
        while (true)
        {
            Console.WriteLine("Enter Search Query:");
            string query = Console.ReadLine();
            command.Parameters["@query"].Value = query;
            Stopwatch stopwatch = Stopwatch.StartNew();
            using SqliteDataReader reader = command.ExecuteReader();
            stopwatch.Stop();
            while (reader.Read())
            {
                Console.WriteLine($"{reader.GetString(0),-28} {reader.GetString(1),-35}{reader.GetString(2)}");
            }
            Console.WriteLine($"Completed in {stopwatch.ElapsedMilliseconds}ms");
        }

    }
}

void CreateDatabase(string databasePath, int numOfTestUsersToGenerate)
{
    var faker = new Faker("en");
    List<Person> people = new List<Person>();
    for (int i = 0; i < numOfTestUsersToGenerate; i++)
    {
        var fakePerson = new Bogus.Person();
        Person person = new Person(
            Ulid.NewUlid().ToString(),
            fakePerson.FirstName,
            faker.Name.FirstName(),
            fakePerson.LastName,
            TakeRandomSelection(5) ? faker.Name.FirstName() : string.Empty,
            fakePerson.Email,
            fakePerson.UserName,
            System.Guid.NewGuid().ToString()
        );
        people.Add(person);
    }

    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C1", "Alice", "Walker", "Jones", "", "Alice.Jones@gmail.COM", "AlJones", "87e74492-66e7-4372-adb0-4f802da4f71"));
    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C7", "Stephen", "Franklin", "Glover Jr.", "", "Stephen.GloverJr.@gmail.COM", "StGloverJr.", "87e74492-66e7-4372-adb0-4f802da4f77"));
    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C8", "Brad", "Edward", "St. Phillips", "", "Brad.St.Phillips@gmail.COM", "BrSt.Phillips", "87e74492-66e7-4372-adb0-4f802da4f78"));
    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C10", "Kurt", "Jadyn", "Smitham", "", "Kurt.Smitham@gmail.COM", "KuSmitham", "87e74492-66e7-4372-adb0-4f802da4f710"));
    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C11", "Charles", "Robert", "Stone", "Chris", "Charles.Stone@gmail.COM", "ChStone", "87e74492-66e7-4372-adb0-4f802da4f711"));
    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C12", "Ann Marie", "Sophia", "Miller", "", "AnnMarie.Miller@gmail.COM", "AnMiller", "87e74492-66e7-4372-adb0-4f802da4f712"));
    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C13", "Mary Beth", "Sophia", "Sutton Chapman", "", "MaryBeth.SuttonChapman@gmail.COM", "MaSuttonChapman", "87e74492-66e7-4372-adb0-4f802da4f713"));
    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C16", "Charlotte", "Isabel", "Smith-Collins", "", "Charlotte.Smith-Collins@gmail.COM", "ChSmith-Collins", "87e74492-66e7-4372-adb0-4f802da4f716"));
    people.Add(new Person("01FAR5W5AZYWCRW9JNHM8N7C19", "William \"Jim\"", "David", "Thomas", "", "Jim.Thomas@gmail.COM", "WiThomas", "87e74492-66e7-4372-adb0-4f802da4f719"));

    CreatePersonTable(databasePath, people);
}

bool TakeRandomSelection(int percentage)
{
    Random rnd = new Random();
    int value = rnd.Next(0, 100);

    if (value < percentage)
    {
        return true;
    }
    return false;
}

void CreatePersonTable(string databasePath, List<Person> people)
{
    using (var connection = new SqliteConnection($"Data Source={databasePath}"))
    {
        connection.Open();
        using (var transaction = connection.BeginTransaction())
        {
            var command = connection.CreateCommand();

            command.CommandText =
            @"CREATE TABLE IF NOT EXISTS person
                (
                    person_id     varchar(26) PRIMARY KEY,
                    first_name    varchar(80),
                    middle_name   varchar(80),
                    last_name     varchar(80),
                    preferred_name varchar(80),
                    person_name   varchar(320),
                    email_address varchar(320),
                    account_name  varchar(320),
                    account_id    varchar(256)
                )";
            command.ExecuteNonQuery();

            command.CommandText =
            @"
        INSERT INTO person(person_id, first_name, middle_name, last_name, preferred_name, person_name, email_address, account_name, account_id)
		VALUES ($person_id, $first_name, $middle_name, $last_name, $preferred_name, $person_name, $email_address, $account_name, $account_id)
    	";
            command.Parameters.Add("$person_id", SqliteType.Text);
            command.Parameters.Add("$first_name", SqliteType.Text);
            command.Parameters.Add("$middle_name", SqliteType.Text);
            command.Parameters.Add("$last_name", SqliteType.Text);
            command.Parameters.Add("$preferred_name", SqliteType.Text);
            command.Parameters.Add("$person_name", SqliteType.Text);
            command.Parameters.Add("$email_address", SqliteType.Text);
            command.Parameters.Add("$account_name", SqliteType.Text);
            command.Parameters.Add("$account_id", SqliteType.Text);
            command.Prepare();
            foreach (var person in people)
            {
                command.Parameters["$person_id"].Value = person.PersonId;
                command.Parameters["$first_name"].Value = person.FirstName;
                command.Parameters["$middle_name"].Value = person.MiddleName;
                command.Parameters["$last_name"].Value = person.LastName;
                command.Parameters["$preferred_name"].Value = person.PreferredName;
                command.Parameters["$person_name"].Value = person.PersonName;
                command.Parameters["$email_address"].Value = person.Email;
                command.Parameters["$account_name"].Value = person.AccountName;
                command.Parameters["$account_id"].Value = person.AccountId;

                command.ExecuteNonQuery();
            }
            transaction.Commit();
        }
    }
}

public record Person(
    string PersonId,
    string FirstName,
    string MiddleName,
    string LastName,
    string PreferredName,
    string Email,
    string AccountName,
    string AccountId)
{
    public string PersonName { get { return $"{FirstName}{(string.IsNullOrWhiteSpace(PreferredName) ? "" : $" ({PreferredName})")}{(string.IsNullOrWhiteSpace(MiddleName) ? "" : $" {MiddleName}")} {LastName}"; } }
}
