// to use Regex and Match
using System.Text.RegularExpressions;
using Amop.Core.Constants;

namespace Amop.Core.Services.RegexService
{
    public class RegexService
    {
        public MatchCollection GetAllUrlsFromText(string text)
        {
            var linkParser = new Regex(RegexConstants.REGEX_MATCH_URL_FROM_EMAIL);
            return linkParser.Matches(text);
        }

        public Match GetFirstNumberFromText(string text)
        {
            var numberParser = new Regex(RegexConstants.REGEX_MATCH_NUMBER);
            return numberParser.Match(text);

        }

        //Get all numbers with n length
        public MatchCollection GetVerificationCodeFromText(string text, int length)
        {
            var linkParser = new Regex(GetRegexMatchVerificationCodeWithLength(length));
            return linkParser.Matches(text);
        }

        // To get verification code from email body
        // Example: "The verification code for user username is 274375."
        private string GetRegexMatchVerificationCodeWithLength(int length)
        {
            return $@"(?<=is )\d{{{length}}}";
        }
    }
}
