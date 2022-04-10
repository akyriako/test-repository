import datetime
import getopt
import shelve
import sys
import concurrent.futures
import time

from timeit import default_timer as timer

import timedelta

import apicaller

date_format = "%Y-%m-%d"
shelve_db_relative_path = "data/results.db"


def main(argv):
    from_input_string = ""
    to_input_string = ""

    usage_message = 'usage(1): main.py -s <startDate> -t <endDate>\n' \
                    'usage(2): main.py --since <startDate> --till <endDate>\n' \
                    'usage(3): main.py --load\n' \
                    'usage(4): main.py --recent <timeDeltaInDays>\n' \
                    'usage(5): main.py --official-stats\n' \
                    'usage(6): main.py --fill\n' \
                    'usage(7): main.py --bootstrap'

    try:
        opts, args = getopt.getopt(argv, "s:t:l:r:a:o:f:b",
                                   ["since=", "till=",
                                    "load",
                                    "recent=",
                                    "analyze",
                                    "official-stats",
                                    "fill",
                                    "bootstrap"])

    except getopt.GetoptError:
        print(usage_message)
        sys.exit(2)

    if len(opts) == 0:
        print(usage_message)
        sys.exit(2)

    try:
        load_shelved_data = False
        load_only_stats = False
        fill_missing_draws = False
        analyze_data = False
        bootstrap = False
        sample_data = {}

        for opt, arg in opts:
            if opt == '-h':
                print(usage_message)
            elif opt in ("-l", "--load"):
                load_shelved_data = True
            elif opt in ("-a", "--analyze"):
                analyze_data = True
            elif opt in ("-r", "--recent"):
                today = datetime.date.today()
                from_input_string = (today - timedelta.Timedelta(days=int(arg))).strftime(date_format)
                to_input_string = today.strftime(date_format)
            elif opt in ("-s", "--since"):
                from_input_string = arg
            elif opt in ("-t", "--till"):
                to_input_string = arg
            elif opt in ("-o", "--official-stats"):
                load_only_stats = True
                load_shelved_data = False
            elif opt in ("-f", "--fill"):
                fill_missing_draws = True
            elif opt in ("-b", "--bootstrap"):
                bootstrap = True

        if load_shelved_data:
            sample_data = get_from_shelve()
        elif not load_shelved_data and not load_only_stats and not fill_missing_draws and not bootstrap:
            sample_data = get_results_by_range(from_input_string, to_input_string)

        if analyze_data:
            print_sample(sample_data)

        if load_only_stats:
            statistics = apicaller.get_statistics()

            for number in statistics["numbers"]:
                print(number)

            print("*" * 80)

            for bonus in statistics["bonus_numbers"]:
                print(bonus)

        if fill_missing_draws:
            get_missing_draws(False)
            # sample_data = get_from_shelve()
            # print_sample(sample_data)

        if bootstrap:
            get_missing_draws(True)

    except Exception as exception:
        print(exception.with_traceback())


def get_results_by_range(from_input_string, to_input_string):
    if len(from_input_string) > 0 and len(to_input_string) > 0:
        try:
            from_input_date = datetime.datetime.strptime(from_input_string, date_format)
            to_input_date = datetime.datetime.strptime(to_input_string, date_format)
        except Exception as exception:
            print("usage: Please enter dates in the following format => {}".format(date_format))

    results = get_draw_results(from_input_date.date(), to_input_date.date())
    return results


def get_draw_results(since, till):
    try:
        results = apicaller.get_draw_by_daterange(since, till)
        put_in_shelve(results)
        return results

    except Exception as api_call_exception:
        print(api_call_exception)


def get_missing_draws(bootstrap):
    start_offset = 0
    stop_offset = 0
    shelved_results_keys_set = {}
    missing_draws = {}

    if not bootstrap:
        shelved_results_keys = sorted(get_from_shelve().keys())
        start_offset = shelved_results_keys[0]
        stop_offset = shelved_results_keys[-1]
    else:
        shelved_results_keys = sorted(get_from_shelve().keys())
        start_offset = 1
        game_id, numbers, bonus = apicaller.get_active_draw()
        stop_offset = game_id

    shelved_results_keys_set = set(shelved_results_keys)
    all_keys = set(range(start_offset, stop_offset + 1))
    missing_keys = all_keys.difference(shelved_results_keys_set)

    if not bootstrap:
        print(f"Retrieving the following missing draws '{missing_keys}':")

    start_bootstrapping_timer = timer()
    offset_size = 10
    offset = 0

    if len(missing_keys) > 0:
        while offset < stop_offset:
            current_subset = list(missing_keys)[offset:offset + offset_size]

            if len(current_subset) > 0:
                print(f"Retrieving: {sorted(current_subset)}")

                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_draw = {executor.submit(apicaller.get_draw_by_id, draw_id): draw_id for draw_id in current_subset}

                    for future in concurrent.futures.as_completed(future_to_draw):
                        draw_id = future_to_draw[future]
                        try:
                            draw = future.result()
                            game_id, numbers, bonus = draw
                            missing_draws[game_id] = numbers, bonus

                            put_in_shelve(missing_draws)
                            missing_draws.clear()
                        except Exception as exception:
                            print('%d generated an exception: %s' % (draw_id, exception))
                        # else:
                        #     print('%d page is %d bytes' % (draw_id, len(draw)))

                offset = offset + offset_size
            else:
                break

            time.sleep(2)

    finish_bootstrapping_timer = timer()
    bootstrapping_timedelta = timedelta.Timedelta(seconds=finish_bootstrapping_timer - start_bootstrapping_timer)
    print(f"\nCompleted retrieving in {bootstrapping_timedelta}")


def put_in_shelve(results):
    with shelve.open(shelve_db_relative_path) as data:
        for game_id in results.keys():
            data[str(game_id)] = results[game_id]


def get_from_shelve():
    shelved_results = {}

    with shelve.open(shelve_db_relative_path) as data:
        for game_id in data.keys():
            shelved_results[int(game_id)] = data[game_id]

    return shelved_results


def print_sample(sample_data):
    if sample_data:
        for draw_id in sorted(sample_data.keys()):
            winning_numbers, bonus = sample_data[draw_id]
            print("{}: Winning Numbers:{} Bonus:{}".format(draw_id, winning_numbers, bonus))


if __name__ == '__main__':
    main(sys.argv[1:])
