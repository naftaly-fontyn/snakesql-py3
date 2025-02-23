"Useful functions for manipulating strings. In particular those in CSV files."

# No imports needed as no regular expressions are used

# Errors
class ParserError(Exception):
    pass

    
# Define functions
def stripEnd(line, whitespace=' '):
    if type(line) == type(''):
        while line and line[-1] == whitespace: 
            line = line[:-1]
        return line
    terms = []
    for term in line:
        while term and term[-1] == whitespace: 
            term = term[:-1]
        terms.append(term)
    return terms
    
def stripStart(line, whitespace=' '):
    if type(line) == type(''):
        while line and line[0] == whitespace: 
            line = line[1:]
        return line
    terms = []
    for term in line:
        while term and term[0] == whitespace: 
            term = term[1:]
        terms.append(term)
    return terms

def stripBoth(line, whitespace=' '):
    if type(line) == type(''):
        return stripStart(stripEnd(line, whitespace))
    else:
        terms = []
        for term in line:
            terms.append(stripStart(stripEnd(term, whitespace)))
        return terms

def splitKeepQuotedValues(string, seperater=' ', quote="'"):
    p = string.split(quote)
    params = []
    for x in range(len(p)):
        if x%2: # ie odd
            params.append(p[x])
        else:
            for param in p[x].split(seperater):
                if param:
                    params.append(param)
    return params

def parseCSV(input_, separater=',', quote='"', linebreak='\n', whitespace=' ', swap={}):
    "Simple CSV parser which expects no spaces between commas and quoted terms."
    if not input_:
        return [[],]
    input_ = input_.decode()
    lines = []
    line = []
    term = ''
    quotedValue = False
    started = False # only needed for quotedValue = False to see whether we are removing whitespace before or after the string of character.
    pos = 0
    while 1:
        if not quotedValue and input_[pos] == quote:           # Start of a new quoted term
            quotedValue=True
            pos += 1
        elif input_[pos:pos+2] == quote+quote:
            term+= quote
            pos += 2
        elif quotedValue and input_[pos:pos+2] == quote+linebreak:   # End of a non-quoted line
            quotedValue=False
            line.append(term)
            term = ''
            lines.append(line)
            line = []
            pos+=2
        elif quotedValue and input_[pos:pos+2] == quote+separater:  # End of a quoted term
            quotedValue=False
            line.append(term)
            term = ''
            pos+=2
            started = False
        elif quotedValue and input_[pos:pos+2] == quote+whitespace:# End of a quoted term
            quotedValue=False
            pos+=2
            while input_[pos] == whitespace:
                pos += 1
        elif quotedValue and input_[pos] == separater:        # comma character
            term+= separater
            pos += 1
        elif not quotedValue and input_[pos:pos+2] == separater+whitespace:    # End of a non-quoted term
            if swap.has_key(term):
                line.append(swap[term])
            else:
                line.append(term)
            term = ''
            started=False
            pos+=2
            while input_[pos] == whitespace:
                pos += 1
        elif not quotedValue and input_[pos] == whitespace:    # End of a non-quoted term
            pos += 1
            while input_[pos] == whitespace:
                pos += 1
            if started and input_[pos] not in [separater, linebreak]:
                soFar = repr(line[-1])
                if not line[-1]:
                    soFar = repr(term)
                elif not term:
                    soFar = 'start of line'
                raise ParserError('Invalid character after %s, term %s in line %s, expected %s or %s characters.'%(str(soFar),  len(line), len(lines)+1, repr(separater), repr(linebreak)))
        elif not quotedValue and input_[pos] == separater:    # End of a non-quoted term
            # if swap.has_key(term):
            if term in swap:
                line.append(swap[term])
            else:
                line.append(term)
            term = ''
            started = False
            pos += 1
        elif quotedValue and input_[pos] == linebreak:        # Linebreak character
            term+= linebreak
            pos += 1
        elif not quotedValue and input_[pos] == linebreak:    # End of a non-quoted line
            # if swap.has_key(term):
            if term in swap:
                line.append(swap[term])
            else:
                line.append(term)
            term = ''
            started = False
            lines.append(line)
            line = []
            pos += 1
        else:                                                # ordinary character
            started = True
            term += input_[pos]
            pos += 1
        if len(input_) <= pos:
            return lines
            
def buildCSV(lines, separater=',', quote='"', linebreak='\n', whitespace=' '):
    result = ''
    for line in lines:
        l = []
        for item in line:
            item = str(item) # Convert to a string
            pos = item.find(quote)
            if pos != -1:
                l.append(quote+item.replace(quote,quote+quote)+quote)
            else:
                pos = item.find(separater)
                if pos != -1:
                    l.append(quote+item+quote)
                else:
                    pos = item.find(linebreak)
                    if pos != -1:
                        l.append(quote+item+quote)
                    else:
                        pos = item.find(whitespace)
                        if pos != -1:
                            l.append(quote+item+quote)
                        else:
                            l.append(item)
        result += separater.join(l)+linebreak
    return result

def smartSplit(term, separater=',', quote='"', linebreak='\n', whitespace=' ', swap={}):
    return parseCSV(term+linebreak, separater, quote, linebreak,  whitespace, swap)[0]
    
def splitPlainList( tables, whitespace=' ', separater=','):
    table = tables.replace(whitespace,'')
    tables = table.split(separater)
    return tables

# Test
if __name__ == '__main__':
    print(smartSplit("'test1', NULL, h ", quote="'", swap={'NULL':None}))
