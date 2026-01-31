const fs = require('fs');
const path = require('path');

// Get the project root directory (go up from backend/public)
const projectRoot = path.join(__dirname, '..');

// Create public directory if it doesn't exist
if (!fs.existsSync(path.join(projectRoot, 'public'))) {
  fs.mkdirSync(path.join(projectRoot, 'public'), { recursive: true });
}

// Copy all HTML files from current directory (backend/public) to public
const htmlFiles = fs.readdirSync(__dirname).filter(file => file.endsWith('.html'));
htmlFiles.forEach(file => {
  const sourcePath = path.join(__dirname, file);
  const destPath = path.join(projectRoot, 'public', file);
  
  let content = fs.readFileSync(sourcePath, 'utf8');
  
  // Replace API_BASE_URL placeholder in dashboard.html
  if (file === 'dashboard.html') {
    const apiUrl = process.env.API_BASE_URL || 'http://localhost:3000';
    content = content.replace(/%%API_BASE_URL%%/g, apiUrl);
    console.log('✅ Dashboard built with API URL:', apiUrl);
  }
  
  fs.writeFileSync(destPath, content);
  console.log('✅ Copied:', file);
});

console.log('🎉 Frontend build completed!');
