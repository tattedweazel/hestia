

def num_format(number, decimals=0):

    if -1000 < number < 1000:
        return str(number)

    elif 1000 <= number < 1000000:
        before_decimal = str(number // 1000)
        if decimals > 0:
            after_decimal = str(number / 1000).split('.')[1][0:decimals]
            if after_decimal in ('0', '00', '000', '0000'):
                return f'{before_decimal}k'
            else:
                return f'{before_decimal}.{after_decimal}k'
        else:
            return f'{before_decimal}k'

    elif 1000000 <= number < 1000000000:
        before_decimal = str(number // 1000000)
        if decimals > 0:
            after_decimal = str(number / 1000000).split('.')[1][0:decimals]
            if after_decimal in ('0', '00', '000', '0000'):
                return f'{before_decimal}M'
            else:
                return f'{before_decimal}.{after_decimal}M'
        else:
            return f'{before_decimal}M'

    elif -1000 >= number > -1000000:
        before_decimal = str(-1 * (number // -1000))
        if decimals > 0:
            after_decimal = str(number / 1000).split('.')[1][0:decimals]
            if after_decimal in ('0', '00', '000', '0000'):
                return f'{before_decimal}k'
            else:
                return f'{before_decimal}.{after_decimal}k'
        else:
            return f'{before_decimal}k'

    elif -1000000 >= number > -1000000000:
        before_decimal = str(-1 * (number // -1000000))
        if decimals > 0:
            after_decimal = str(number / 1000000).split('.')[1][0:decimals]
            if after_decimal in ('0', '00', '000', '0000'):
                return f'{before_decimal}M'
            else:
                return f'{before_decimal}.{after_decimal}M'
        else:
            return f'{before_decimal}M'
