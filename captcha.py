import utils
import pendulum
import requests
import os

API_KEY = '4CAY5SBFO1GLCR3HMPS8PHGJXF7PW1TV7E2KOTLB'#######


@utils.retryable(max_retries=9, sleeptime=20)
def get_captcha_response(logger, site_key, site_url):
    for n in range(1,11):
        headers= {
            'User-Agent':'PostmanRuntime/7.29.0',
            'Accept':'*/*',
            'Accept-Encoding':'gzip, deflate, br',
            'Connection':'keep-alive'
        }
        data = {
            'action':'upload',
            'key': API_KEY,
            'captchatype':3,
            'gen_task_id':int(pendulum.now().format('x')),
            'sitekey':site_key,
            'pageurl':site_url,
        }
        logger.info(f'Fetching ReCaptcha response...')
        response = requests.post('http://fasttypers.org/Imagepost.ashx', data=data, headers=headers)#, verify=False)
        if response.status_code == 200 and len(response.text) > 50:
            logger.info(f'ReCaptcha Success: {response.text[:10]}...')
            return response.text
        else:
            logger.info(f'ReCaptcha Failure: Attempt #{n}...')
            # raise utils.PleaseRetryException

def solve_recaptcha(browser, logger, site_key):
    google_captcha_response_input = browser.driver.find_element_by_id('g-recaptcha-response')
    # make input visible
    browser.driver.execute_script(
        "arguments[0].setAttribute('style','type: text; visibility:visible;');",
        google_captcha_response_input)
    # input the code received from 2captcha API
    google_captcha_response_input.send_keys(
        get_captcha_response(logger, site_key, browser.current_url()))
    # hide the captcha input
    browser.driver.execute_script(
        "arguments[0].setAttribute('style', 'display:none;');",
        google_captcha_response_input)