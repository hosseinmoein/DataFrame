// Hossein Moein
// March 18, 2018
/*
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <DataFrame/Utils/DateTime.h>
#include <DataFrame/Utils/FixedSizePriorityQueue.h>
#include <DataFrame/Utils/FixedSizeString.h>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>

using namespace hmdf;

// ----------------------------------------------------------------------------

static void test_priority_queue()  {

    FixedSizePriorityQueue<double, 5>   q;

    q.push(3);
    assert(q.top() == 3);
    assert(q.size() == 1);
    assert((q.data() == std::vector<double> { 3 }));
    q.push(7);
    assert(q.top() == 7);
    assert(q.size() == 2);
    assert((q.data() == std::vector<double> { 3, 7 }));
    q.push(5);
    assert(q.top() == 7);
    assert(q.size() == 3);
    assert((q.data() == std::vector<double> { 3, 5, 7 }));

    q.push(10);
    q.push(12);
    q.push(15);
    assert(q.top() == 15);
    assert(q.size() == 5);
    assert((q.data() == std::vector<double> { 5, 7, 10, 12, 15 }));

    q.pop();
    assert(q.top() == 12);
    assert(q.size() == 4);
    assert((q.data() == std::vector<double> { 5, 7, 10, 12 }));
    q.pop();
    assert(q.top() == 10);
    assert(q.size() == 3);
    assert((q.data() == std::vector<double> { 5, 7, 10 }));
    q.pop();
    q.pop();
    q.pop();
    q.pop();
    q.pop();
    q.pop();
    assert(q.size() == 0);
    assert(q.empty());
    assert((q.data() == std::vector<double> {  }));
}

// ----------------------------------------------------------------------------

int main (int argc, char *argv [])  {

    using namespace std;

    String1K    tstr;

    DateTime    now;
    DateTime    gmnow (DT_TIME_ZONE::GMT);

    cout << "Local Time is: " << now.string_format (DT_FORMAT::DT_TM2)
         << "   Nanoseconds: " << now.nanosec () << endl;
    cout << "GMT Time is: " << gmnow.string_format (DT_FORMAT::DT_TM2)
         << "   Microseconds: " << now.microsec () << endl;
    cout << "Local Time is: " << now.string_format (DT_FORMAT::ISO_DT) << endl;
    cout << "Local Time is: " << now.string_format (DT_FORMAT::ISO_DT_TM)
         << endl;

    now = 19721202;
    assert(now.string_format(DT_FORMAT::DT_TM2) == "12/02/1972 00:00:00.000");
    now = "19910223 16:23:45.230";
    assert(now.string_format(DT_FORMAT::DT_TM2) == "02/23/1991 16:23:45.230");
    now = "19910223 16:23:45.230";
    assert(now.string_format(DT_FORMAT::DT_TM2) == "02/23/1991 16:23:45.230");
    now = "20100207 12";
    assert(now.string_format(DT_FORMAT::DT_TM2) == "02/07/2010 12:00:00.000");
    now = "20100207 12:32";
    assert(now.string_format(DT_FORMAT::DT_TM2) == "02/07/2010 12:32:00.000");
    now = "20100207 12:32:12";

    try  {
        now.diff_seconds (gmnow);

        assert(false);
    }
    catch (const std::runtime_error &)  {
        // cout << ex.what();
        ; // Time diff between different timezones is not implemented
    }

    {
        // Testing DateTime's add_days()

        DateTime    now (20181023, 10, 24, 1, 123456789);

        assert(now.string_format(DT_FORMAT::DT_TM2) ==
                   "10/23/2018 10:24:01.123");
        now.add_days (1);
        assert(now.string_format(DT_FORMAT::DT_TM2) ==
                   "10/24/2018 10:24:01.123");
        now.add_days (-1);
        assert(now.string_format(DT_FORMAT::DT_TM2) ==
                   "10/23/2018 10:24:01.123");

        now.add_weekdays (1);
        assert(now.string_format(DT_FORMAT::DT_TM2) ==
                   "10/24/2018 10:24:01.123");
        now.add_weekdays (-1);
        assert(now.string_format(DT_FORMAT::DT_TM2) ==
                   "10/23/2018 10:24:01.123");

        {
            DateTime    dt (20091224, 9, 30);

            dt.add_weekdays (2);
            assert(dt.string_format (DT_FORMAT::DT_TM2) ==
                       "12/28/2009 09:30:00.000");
        }

        {
            DateTime    dt (20081224, 9, 30);

            dt.add_weekdays (2);
            assert(dt.string_format (DT_FORMAT::DT_TM2) ==
                       "12/26/2008 09:30:00.000");
        }

        {
            DateTime    dt (20061224, 9, 30);

            dt.add_weekdays (2);
            assert(dt.string_format (DT_FORMAT::DT_TM2) ==
                       "12/26/2006 09:30:00.000");
        }

        {
            DateTime    dt (20051223, 9, 30);

            dt.add_weekdays (2);
            assert(dt.string_format (DT_FORMAT::DT_TM2) ==
                       "12/27/2005 09:30:00.000");
        }

        {
            DateTime    dt (20041223, 9, 30);

            dt.add_weekdays (2);
            assert(dt.string_format (DT_FORMAT::DT_TM2) ==
                       "12/27/2004 09:30:00.000");
        }

        {
            DateTime    dt (20001222, 9, 30);

            dt.add_weekdays (2);
            assert(dt.string_format (DT_FORMAT::DT_TM2) ==
                       "12/26/2000 09:30:00.000");
        }

        {
            DateTime    dt (19981224, 9, 30);

            dt.add_weekdays (2);
            assert(dt.string_format (DT_FORMAT::DT_TM2) ==
                       "12/28/1998 09:30:00.000");
        }
    }

    {
        cout << "\n----- Testing DateTime's GMT vs Local time\n" << endl;

        DateTime    gmt_now (DT_TIME_ZONE::GMT);
        DateTime    local_now;

        DateTime    local_alt_con (local_now.date (), local_now.hour (),
                                   local_now.minute (), local_now.sec (),
                                   local_now.msec ());
        DateTime    gmt_alt_con (gmt_now.date (), gmt_now.hour (),
                                 gmt_now.minute (), gmt_now.sec (),
                                 gmt_now.msec (), DT_TIME_ZONE::GMT);

        DateTime    local_time_1970 (19700101);
        DateTime    gmt_time_1970 (19700101, 0, 0, 0, 0, DT_TIME_ZONE::GMT);

        DateTime    local_time_1989 (19891214, 20, 15, 23, 0);
        DateTime    gmt_time_1989 (19891214, 20, 15, 23, 0, DT_TIME_ZONE::GMT);

        cout << "GMT now time: " << gmt_now.string_format (DT_FORMAT::DT_TM2)
             << "   " << gmt_now.time () << endl;
        cout << "GMT ALT Constructor time: "
             << gmt_alt_con.string_format (DT_FORMAT::DT_TM2)
             << "   " << gmt_alt_con.time () << endl << endl;

        cout << "Local now time: "
             << local_now.string_format (DT_FORMAT::DT_TM2)
             << "   " << local_now.time ()  << endl;
        cout << "Local ALT Constructor time: "
             << local_alt_con.string_format (DT_FORMAT::DT_TM2)
             << "   " << local_alt_con.time () << endl << endl;

        cout << "Local 1970 ALT Constructor time: "
             << local_time_1970.string_format (DT_FORMAT::DT_TM2)
             << "   " << local_time_1970.time () << endl;
        cout << "GMT 1970 ALT Constructor time: "
             << gmt_time_1970.string_format (DT_FORMAT::DT_TM2)
             << "   " << gmt_time_1970.time () << endl << endl;

        cout << "Local 1989 ALT Constructor time: "
             << local_time_1989.string_format (DT_FORMAT::DT_TM2)
             << "   " << local_time_1989.time () << endl;
        cout << "GMT 1989 ALT Constructor time: "
             << gmt_time_1989.string_format (DT_FORMAT::DT_TM2)
             << "   " << gmt_time_1989.time () << endl << endl;
    }

    {
        cout << "\n----- Testing DateTime's weekdays day methods\n" << endl;

        const DateTime  new_year (20020101);

        cout << "20020101 is a US holiday: "
             << (new_year.is_us_business_day () ? "NO" : "YES") << endl;

        const DateTime  thanksgiving (20011122);

        cout << "20011122 (Thanksgiving) is a US holiday: "
             << (thanksgiving.is_us_business_day () ? "NO" : "YES") << endl;

        DateTime        yes_answers [80];
        int             i = 0;
        const DateTime  hol01 (19980101);  yes_answers[i++] = hol01;
        const DateTime  hol02 (19980119);  yes_answers[i++] = hol02;
        const DateTime  hol03 (19980216);  yes_answers[i++] = hol03;
        const DateTime  hol04 (19980525);  yes_answers[i++] = hol04;
        const DateTime  hol05 (19980703);  yes_answers[i++] = hol05;
        const DateTime  hol06 (19980907);  yes_answers[i++] = hol06;
        const DateTime  hol07 (19981126);  yes_answers[i++] = hol07;
        const DateTime  hol08 (19981225);  yes_answers[i++] = hol08;
        const DateTime  hol09 (19990101);  yes_answers[i++] = hol09;
        const DateTime  hol10 (19990118);  yes_answers[i++] = hol10;
        const DateTime  hol11 (19990215);  yes_answers[i++] = hol11;
        const DateTime  hol12 (19990531);  yes_answers[i++] = hol12;
        const DateTime  hol13 (19990705);  yes_answers[i++] = hol13;
        const DateTime  hol14 (19990906);  yes_answers[i++] = hol14;
        const DateTime  hol15 (19991125);  yes_answers[i++] = hol15;
        const DateTime  hol16 (19991224);  yes_answers[i++] = hol16;
        const DateTime  hol17 (19991231);  yes_answers[i++] = hol17;
        const DateTime  hol18 (20000117);  yes_answers[i++] = hol18;
        const DateTime  hol19 (20000221);  yes_answers[i++] = hol19;
        const DateTime  hol20 (20000529);  yes_answers[i++] = hol20;
        const DateTime  hol21 (20000704);  yes_answers[i++] = hol21;
        const DateTime  hol22 (20000904);  yes_answers[i++] = hol22;
        const DateTime  hol23 (20001123);  yes_answers[i++] = hol23;
        const DateTime  hol24 (20001225);  yes_answers[i++] = hol24;
        const DateTime  hol25 (20010101);  yes_answers[i++] = hol25;
        const DateTime  hol26 (20010115);  yes_answers[i++] = hol26;
        const DateTime  hol27 (20010219);  yes_answers[i++] = hol27;
        const DateTime  hol28 (20010528);  yes_answers[i++] = hol28;
        const DateTime  hol29 (20010704);  yes_answers[i++] = hol29;
        const DateTime  hol30 (20010903);  yes_answers[i++] = hol30;
        const DateTime  hol31 (20011122);  yes_answers[i++] = hol31;
        const DateTime  hol32 (20011225);  yes_answers[i++] = hol32;
        const DateTime  hol33 (20020101);  yes_answers[i++] = hol33;
        const DateTime  hol34 (20020121);  yes_answers[i++] = hol34;
        const DateTime  hol35 (20020218);  yes_answers[i++] = hol35;
        const DateTime  hol36 (20020527);  yes_answers[i++] = hol36;
        const DateTime  hol37 (20020704);  yes_answers[i++] = hol37;
        const DateTime  hol38 (20020902);  yes_answers[i++] = hol38;
        const DateTime  hol39 (20021128);  yes_answers[i++] = hol39;
        const DateTime  hol40 (20021225);  yes_answers[i++] = hol40;
        const DateTime  hol41 (20030101);  yes_answers[i++] = hol41;
        const DateTime  hol42 (20030120);  yes_answers[i++] = hol42;
        const DateTime  hol43 (20030217);  yes_answers[i++] = hol43;
        const DateTime  hol44 (20030526);  yes_answers[i++] = hol44;
        const DateTime  hol45 (20030704);  yes_answers[i++] = hol45;
        const DateTime  hol46 (20030901);  yes_answers[i++] = hol46;
        const DateTime  hol47 (20031127);  yes_answers[i++] = hol47;
        const DateTime  hol48 (20031225);  yes_answers[i++] = hol48;
        const DateTime  hol49 (20040101);  yes_answers[i++] = hol49;
        const DateTime  hol50 (20040119);  yes_answers[i++] = hol50;
        const DateTime  hol51 (20040216);  yes_answers[i++] = hol51;
        const DateTime  hol52 (20040531);  yes_answers[i++] = hol52;
        const DateTime  hol53 (20040705);  yes_answers[i++] = hol53;
        const DateTime  hol54 (20040906);  yes_answers[i++] = hol54;
        const DateTime  hol55 (20041125);  yes_answers[i++] = hol55;
        const DateTime  hol56 (20041224);  yes_answers[i++] = hol56;
        const DateTime  hol57 (20041231);  yes_answers[i++] = hol57;
        const DateTime  hol58 (20050117);  yes_answers[i++] = hol58;
        const DateTime  hol59 (20050221);  yes_answers[i++] = hol59;
        const DateTime  hol60 (20050530);  yes_answers[i++] = hol60;
        const DateTime  hol61 (20050704);  yes_answers[i++] = hol61;
        const DateTime  hol62 (20050905);  yes_answers[i++] = hol62;
        const DateTime  hol63 (20051124);  yes_answers[i++] = hol63;
        const DateTime  hol64 (20051226);  yes_answers[i++] = hol64;
        const DateTime  hol65 (20060102);  yes_answers[i++] = hol65;
        const DateTime  hol66 (20060116);  yes_answers[i++] = hol66;
        const DateTime  hol67 (20060220);  yes_answers[i++] = hol67;
        const DateTime  hol68 (20060529);  yes_answers[i++] = hol68;
        const DateTime  hol69 (20060704);  yes_answers[i++] = hol69;
        const DateTime  hol70 (20060904);  yes_answers[i++] = hol70;
        const DateTime  hol71 (20061123);  yes_answers[i++] = hol71;
        const DateTime  hol72 (20061225);  yes_answers[i++] = hol72;
        const DateTime  hol73 (20070101);  yes_answers[i++] = hol73;
        const DateTime  hol74 (20070115);  yes_answers[i++] = hol74;
        const DateTime  hol75 (20070219);  yes_answers[i++] = hol75;
        const DateTime  hol76 (20070528);  yes_answers[i++] = hol76;
        const DateTime  hol77 (20070704);  yes_answers[i++] = hol77;
        const DateTime  hol78 (20070903);  yes_answers[i++] = hol78;
        const DateTime  hol79 (20071122);  yes_answers[i++] = hol79;
        const DateTime  hol80 (20071225);  yes_answers[i++] = hol80;

        for (i = 0; i < 80; i++)  {
            cout << (yes_answers [i].is_us_business_day ()
                        ?  (yes_answers [i].string_format (DT_FORMAT::DT_TM2) +
                           " should be a holiday\n")
                        : "");
        }

        for (i = 0; i < 80; i++)  {
            for (int j = 0; j < 7; j++)  {
                yes_answers [i].add_days (1);
                if (! yes_answers [i].is_weekend () &&
                    ! yes_answers [i].is_us_business_day ())  {
                    cout << yes_answers [i].string_format (DT_FORMAT::DT_TM2)
                         << " may not be a holiday" << endl;
                }
            }
            yes_answers [i].add_days (-7);
            if (yes_answers [i].is_us_business_day ())  {
                cout << yes_answers [i].string_format (DT_FORMAT::DT_TM2)
                     << " should be a holiday" << endl;
            }
            for (int j = 0; j < 7; j++)  {
                yes_answers [i].add_days (-1);
                if (! yes_answers [i].is_weekend () &&
                    ! yes_answers [i].is_us_business_day ())  {
                    cout << yes_answers [i].string_format (DT_FORMAT::DT_TM2)
                         << " may not be a holiday" << endl;
                }
            }
        }

        i = 0;

        DateTime        columbus [10];
        const DateTime  columbus01 (19981012);  columbus[i++] = columbus01;
        const DateTime  columbus02 (19991011);  columbus[i++] = columbus02;
        const DateTime  columbus03 (20001009);  columbus[i++] = columbus03;
        const DateTime  columbus04 (20011008);  columbus[i++] = columbus04;
        const DateTime  columbus05 (20021014);  columbus[i++] = columbus05;
        const DateTime  columbus06 (20031013);  columbus[i++] = columbus06;
        const DateTime  columbus07 (20041011);  columbus[i++] = columbus07;
        const DateTime  columbus08 (20051010);  columbus[i++] = columbus08;
        const DateTime  columbus09 (20061009);  columbus[i++] = columbus09;
        const DateTime  columbus10 (20071008);  columbus[i++] = columbus10;

        for (i = 0; i < 10; i++)  {
            cout << (columbus [i].is_us_bank_holiday ()
                        ? ""
                        : (columbus [i].string_format (DT_FORMAT::DT_TM2) +
                          " should be a bank holiday\n"));
            cout << (columbus [i].is_us_business_day ()
                        ? ""
                        : (columbus [i].string_format (DT_FORMAT::DT_TM2) +
                          " should not be a regular holiday\n"));
        }
        for (i = 0; i < 10; i++)  {
            for (int j = 0; j < 7; j++)  {
                columbus [i].add_days (1);
                if (! columbus [i].is_weekend () &&
                    columbus [i].is_us_bank_holiday ())  {
                    cout << columbus [i].string_format (DT_FORMAT::DT_TM2)
                         << " should not be a bank holiday" << endl;
                }
                if (! columbus [i].is_weekend () &&
                    ! columbus [i].is_us_business_day ())  {
                    cout << columbus [i].string_format (DT_FORMAT::DT_TM2)
                         << " may not be a holiday" << endl;
                }
            }
            columbus [i].add_days (-7);
            if (! columbus [i].is_us_bank_holiday ())  {
                cout << columbus [i].string_format (DT_FORMAT::DT_TM2)
                     << " should be a regular holiday" << endl;
            }
            if (! columbus [i].is_us_business_day ())  {
                cout << columbus [i].string_format (DT_FORMAT::DT_TM2)
                     << " should not be a regular holiday" << endl;
            }
            for (int j = 0; j < 7; j++)  {
                columbus [i].add_days (-1);
                if (! columbus [i].is_weekend () &&
                    columbus [i].is_us_bank_holiday ())  {
                    cout << columbus [i].string_format (DT_FORMAT::DT_TM2)
                         << " should not be a bank holiday" << endl;
                }
                if (! columbus [i].is_weekend () &&
                    ! columbus [i].is_us_business_day ())  {
                    cout << columbus [i].string_format (DT_FORMAT::DT_TM2)
                         << " may not be a holiday" << endl;
                }
            }
        }
    }

    cout << "\n----- Testing DateTime's diff methods\n" << endl;

    {
        const DateTime  dt1 (20020101, 12, 54, 31);
        const DateTime  dt2 (20020201, 3, 50, 30);

        cout << "dt1 is: " << dt1.string_format (DT_FORMAT::DT_TM2) << endl;
        cout << "dt2 is: " << dt2.string_format (DT_FORMAT::DT_TM2) << endl;
        cout << "second difference between dt1 and dt2: "
             << dt1.diff_seconds (dt2) << endl;
        cout << "minute difference between dt1 and dt2: "
             << dt1.diff_minutes (dt2) << endl;
        cout << "hour difference between dt1 and dt2: "
             << dt1.diff_hours (dt2) << endl;
        cout << "day difference between dt1 and dt2: "
             << dt1.diff_days (dt2) << endl;
        cout << "business day difference between dt1 and dt2: "
             << dt1.diff_weekdays (dt2) << endl;
        cout << "week difference between dt1 and dt2: "
             << dt1.diff_weeks (dt2) << endl;

        size_t          index = 0;
        const size_t    max_index = 10000;
        DateTime        now_performance (19700101);

        while (++index < max_index)
            now_performance.add_weekdays (1);

        cout << "After adding " << max_index << " weekdays days to 19700101, "
             << "the date is "
             << now_performance.string_format (DT_FORMAT::DT_TM2)
             << endl;
    }

    cout << "\n----- Testing DateTime's DST vs EST (Going Forward)\n" << endl;
    {
        {
            DateTime    start (20060331);
            DateTime    test;
            int         i = 0;

            cout << "\tGoing from EST to DST\n" << endl;
            while (i++ <= 10)  {
                test = start;
                cout << test.string_format (DT_FORMAT::DT_TM2)
                     << " --- " << test.time () << endl;
                start.add_days (1);
            }
        }

        {
            DateTime    start (20051028);
            DateTime    test;
            int         i = 0;

            cout << endl << endl;
            cout << "\tGoing from DST to EST\n" << endl;
            while (i++ <= 10)  {
                test = start;
                cout << test.string_format (DT_FORMAT::DT_TM2)
                     << " --- " << test.time () << endl;
                start.add_days (1);
            }
        }

        {
            DateTime    start (20060331);

            cout << endl << endl;
            cout << "\tGoing from EST to DST in one shot\n" << endl;

            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
            start.add_days (10);
            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
        }

        {
            DateTime    start (20051028);

            cout << endl << endl;
            cout << "\tGoing from DST to EST in one shot\n" << endl;

            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
            start.add_days (10);
            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
        }
    }


    cout << "\n----- Testing DateTime's DST vs EST (Going Backward)\n" << endl;

    {
        {
            DateTime    start (20060408);
            DateTime    test;
            int         i = 0;

            cout << "\tGoing from EST to DST\n" << endl;
            while (i++ <= 10)  {
                test = start;
                cout << test.string_format (DT_FORMAT::DT_TM2)
                     << " --- " << test.time () << endl;
                start.add_days (-1);
            }
        }

        {
            DateTime    start (20051105);
            DateTime    test;
            int         i = 0;

            cout << endl << endl;
            cout << "\tGoing from DST to EST\n" << endl;
            while (i++ <= 10)  {
                test = start;
                cout << test.string_format (DT_FORMAT::DT_TM2)
                     << " --- " << test.time () << endl;
                start.add_days (-1);
            }
        }

        {
            DateTime    start (20060408);

            cout << endl << endl;
            cout << "\tGoing from EST to DST in one shot\n" << endl;

            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
            start.add_days (-10);
            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
        }

        {
            DateTime    start (20051105);

            cout << endl << endl;
            cout << "\tGoing from DST to EST in one shot\n" << endl;

            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
            start.add_days (-10);
            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
        }

        {
            DateTime    start (20090309, 16, 30);

            cout << endl << endl;
            cout << "\tGoing from EST to DST in one shot (2)\n" << endl;

            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
            start.add_days (-20);
            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
        }

        {
            DateTime    start (20081103, 16, 30);

            cout << endl << endl;
            cout << "\tGoing from DST to EST in one shot (3)\n" << endl;

            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
            start.add_weekdays (-1);
            cout << start.string_format (DT_FORMAT::DT_TM2)
                 << " --- " << start.time () << endl;
        }
    }

    {
        cout << "\n----- Testing DateTime going over year boundary\n" << endl;

        {
            DateTime    test (20081225, 16, 30);
            int         i = 0;

            cout << endl << endl;
            cout << "\tGoing Forward\n" << endl;

            while (i++ <= 10)  {
                cout << test.string_format (DT_FORMAT::DT_TM2)
                     << " --- " << test.time () << endl;
                test.add_days (1);
            }
        }
    }

    {
        {
            DateTime    test (20080105, 16, 30);
            int         i = 0;

            cout << endl << endl;
            cout << "\tGoing Backward\n" << endl;

            while (i++ <= 10)  {
                cout << test.string_format (DT_FORMAT::DT_TM2)
                     << " --- " << test.time () << endl;
                test.add_days (-1);
            }
        }
    }

    {
        cout << "\n----- Testing DateTime's time zone changes\n" << endl;

        DateTime    local_now;
        DateTime    gmt_now (DT_TIME_ZONE::GMT);
        DateTime    b_aires_now (DT_TIME_ZONE::AM_BUENOS_AIRES);
        DateTime    tehran_now (DT_TIME_ZONE::AS_TEHRAN);
        DateTime    tokyo_now (DT_TIME_ZONE::AS_TOKYO);
        DateTime    sydney_now (DT_TIME_ZONE::AU_SYDNEY);
        DateTime    nz_now (DT_TIME_ZONE::NZ);
        DateTime    moscow_now (DT_TIME_ZONE::EU_MOSCOW);
        DateTime    shang_now (DT_TIME_ZONE::AS_SHANGHAI);
        DateTime    berlin_now (DT_TIME_ZONE::EU_BERLIN);
        DateTime    paris_now (DT_TIME_ZONE::EU_PARIS);
        DateTime    stock_now (DT_TIME_ZONE::EU_STOCKHOLM);
        DateTime    ny_now (DT_TIME_ZONE::AM_NEW_YORK);
        DateTime    la_now (DT_TIME_ZONE::AM_LOS_ANGELES);
        DateTime    local_now_2;

        cout << "Now Local:        " << local_now << endl;
        cout << "GMT Local:        " << gmt_now << endl;
        cout << "Now Buenos Aires: " << b_aires_now << endl;
        cout << "Now Tehran:       " << tehran_now << endl;
        cout << "Now Tokyo:        " << tokyo_now << endl;
        cout << "Now Sydeny:       " << sydney_now << endl;
        cout << "Now New Zealand:  " << nz_now << endl;
        cout << "Now Moscow:       " << moscow_now << endl;
        cout << "Now Shanghai:     " << shang_now << endl;
        cout << "Now Berlin:       " << berlin_now << endl;
        cout << "Now Paris:        " << paris_now << endl;
        cout << "Now Stockholm:    " << stock_now << endl;
        cout << "Now New York:     " << ny_now << endl;
        cout << "Now Los Amgeles:  " << la_now << endl;
        cout << "Now Local (2):    " << local_now_2 << endl;

        local_now.set_timezone (DT_TIME_ZONE::AU_SYDNEY);
        cout << "\nGoing from Local to Sydney: " << local_now << endl;

        DateTime    local (20091123, 9, 38, 43, 345);

        cout << "\nLocal Date/Time: " << local << endl;
        local.set_timezone (DT_TIME_ZONE::AU_SYDNEY);
        cout << "\nLocal changed to Sydney: " << local << endl;
    }

    {
        // Testing DateTime's const char * constructor

        const DateTime  dt1 ("20100207 12:31");

        if (dt1.string_format (DT_FORMAT::DT_TM2) !=
                "02/07/2010 12:31:00.000")  {
            std::cout << "ERROR: "
                      << dt1.string_format (DT_FORMAT::DT_TM2) << " != "
                      << "02/07/2010 12:31:00.000" << std::endl;
            return (EXIT_FAILURE);
        }

        const DateTime  dt2 ("02/08/2010 12:31:56", DT_DATE_STYLE::AME_STYLE);

        if (dt2.string_format (DT_FORMAT::DT_TM2) !=
                "02/08/2010 12:31:56.000")  {
            std::cout << "ERROR: "
                      << dt2.string_format (DT_FORMAT::DT_TM2) << " != "
                      << "02/08/2010 12:31:56.000" << std::endl;
            return (EXIT_FAILURE);
        }

        const DateTime  dt3 ("2010/02/09 12:31:56", DT_DATE_STYLE::EUR_STYLE);

        if (dt3.string_format (DT_FORMAT::DT_TM2) !=
                "02/09/2010 12:31:56.000")  {
            std::cout << "ERROR: "
                      << dt3.string_format (DT_FORMAT::DT_TM2) << " != "
                      << "02/09/2010 12:31:56.000" << std::endl;
            return (EXIT_FAILURE);
        }

        const DateTime  dt4 ("  2010/02/10 12:31:56", DT_DATE_STYLE::EUR_STYLE);

        if (dt4.string_format (DT_FORMAT::DT_TM2) !=
                "02/10/2010 12:31:56.000")  {
            std::cout << "ERROR: "
                      << dt4.string_format (DT_FORMAT::DT_TM2) << " != "
                      << "02/10/2010 12:31:56.000" << std::endl;
            return (EXIT_FAILURE);
        }

        const DateTime  dt5 ("    02/11/2010 12:31:56",
                             DT_DATE_STYLE::AME_STYLE);

        if (dt5.string_format (DT_FORMAT::DT_TM2) !=
                "02/11/2010 12:31:56.000")  {
            std::cout << "ERROR: "
                      << dt5.string_format (DT_FORMAT::DT_TM2) << " != "
                      << "02/11/2010 12:31:56.000" << std::endl;
            return (EXIT_FAILURE);
        }

        const DateTime dt6 (20190110, 13, 56, 23, 123456987);

        if (dt6.string_format (DT_FORMAT::DT_PRECISE) !=
                "1547146583.123456987")  {
            std::cout << "ERROR: "
                      << dt6.string_format (DT_FORMAT::DT_PRECISE) << " != "
                      << "1547146583.123456987" << std::endl;
            // return (EXIT_FAILURE);
        }
    }

    {
        // Testing DateTime's add_months()

        DateTime    now (20190530, 11, 15, 5, 123456789);

        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:05.123");

        now.add_months (1);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "06/30/2019 11:15:05.123");

        now.add_months (-1);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:05.123");

        now.add_months (10);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "03/30/2020 11:15:05.123");

        now.add_months (-10);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:05.123");

        now.add_months (9);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "02/29/2020 11:15:05.123");
    }

    {
        // Testing DateTime's add_years()

        DateTime    now (20160229, 11, 15, 5, 123456789);

        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "02/29/2016 11:15:05.123");

        now.add_years (1);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "02/28/2017 11:15:05.123");

        now.add_years (-1);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "02/28/2016 11:15:05.123");

        now.add_years (5);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "02/28/2021 11:15:05.123");

        now.add_years (-5);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "02/28/2016 11:15:05.123");

        now.add_years (9);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "02/28/2025 11:15:05.123");
    }

    {
        // Testing DateTime's long_time()

        const DateTime  dt1 (20190522, 11, 15, 5, 123456789,
                             DT_TIME_ZONE::AM_NEW_YORK);

        assert(dt1.long_time() == 1558538105123456789);

        const DateTime  dt2 (20190522, 11, 15, 5, 5,
                             DT_TIME_ZONE::AM_NEW_YORK);

        assert(dt2.long_time() == 1558538105000000005);

        const DateTime  dt3 (20190522, 11, 15, 0, 0,
                             DT_TIME_ZONE::AM_NEW_YORK);

        assert(dt3.long_time() == 1558538100000000000);
        assert(dt3.time() == 1558538100);
    }

    {
        // Testing DateTime's add_nanoseconds()

        DateTime    now (20190530, 11, 15, 5, 123000000,
                         DT_TIME_ZONE::AM_NEW_YORK);

        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:05.123");

        now.add_nanoseconds (1000000000);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:06.123");
        assert(now.long_time() == 1559229306123000000);
        now.add_nanoseconds (-1000000000);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:05.123");
        assert(now.long_time() == 1559229305123000000);
        now.add_nanoseconds (1000010000);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:06.123");
        assert(now.long_time() == 1559229306123010000);
        now.add_nanoseconds (-1000010000);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:05.123");
        assert(now.long_time() == 1559229305123000000);

        now.add_nanoseconds (9999);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:05.123");
        assert(now.long_time() == 1559229305123009999);
        now.add_nanoseconds (-9999);
        assert(now.string_format (DT_FORMAT::DT_TM2) ==
                   "05/30/2019 11:15:05.123");
        assert(now.long_time() == 1559229305123000000);

        now.add_nanoseconds (-123000000);
        assert(now.long_time() == 1559229305000000000);
        now.add_nanoseconds (500000000);
        assert(now.long_time() == 1559229305500000000);
        now.add_nanoseconds (500000000);
        assert(now.long_time() == 1559229306000000000);
        now.add_nanoseconds (500000000);
        assert(now.long_time() == 1559229306500000000);
        now.add_nanoseconds (500000000);
        assert(now.long_time() == 1559229307000000000);

        now.add_nanoseconds (-500000000);
        assert(now.long_time() == 1559229306500000000);
        now.add_nanoseconds (-500000000);
        assert(now.long_time() == 1559229306000000000);
        now.add_nanoseconds (-500000000);
        assert(now.long_time() == 1559229305500000000);
    }

    {
        // Testing DateTime's hash value

        DateTime    now (20190530, 11, 15, 5, 123000000,
                         DT_TIME_ZONE::AM_NEW_YORK);

        std::cout << "Hash Value:      " << std::hash<DateTime>()(now)
                  << std::endl;
        std::cout << "Long Time Value: " << now.long_time() << std::endl;
        now.add_nanoseconds (1040506079);
        std::cout << "Hash Value:      " << std::hash<DateTime>()(now)
                  << std::endl;
        std::cout << "Long Time Value: " << now.long_time() << std::endl;
    }

    {
        DateTime    dt("2015/01/05 09:40:00", hmdf::DT_DATE_STYLE::EUR_STYLE);
        DateTime    dt2;

        dt2 = "20150105 09:40:00";
        assert(dt == dt2);
    }

    {
        DateTime    dt("2015/01/05 09:40:30", hmdf::DT_DATE_STYLE::EUR_STYLE);
        DateTime    dt2("2015-01-05 09:40:30", hmdf::DT_DATE_STYLE::ISO_STYLE);
        DateTime    dt3("2015/01/05 09:40:30.123",
                       hmdf::DT_DATE_STYLE::EUR_STYLE);
        DateTime    dt4("2015-01-05 09:40:30.123",
                       hmdf::DT_DATE_STYLE::ISO_STYLE);

        assert(dt == dt2);
        assert(dt3 == dt4);
    }

    test_priority_queue();
    return (EXIT_SUCCESS);
}

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
