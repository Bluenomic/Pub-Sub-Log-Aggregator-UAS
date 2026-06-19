import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
    // A number specifying the number of VUs to run concurrently.
    vus: 10,
    // A string specifying the total duration of the test run.
    duration: '10s',
};


export default function() {
    const uniqueId = new Date().getTime();
    const body = {
        username: `user-${uniqueId}`,
        password: 'rahasia',
        name: 'PZN',
    };
    http.post('http://localhost:3000/api/users', JSON.stringify(body), {
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
    })
}
